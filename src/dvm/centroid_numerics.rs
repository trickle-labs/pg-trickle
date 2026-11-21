//! Conservative error checks for incremental pgvector averages.
//!
//! The magnitude for a component is the sum of the absolute values of all
//! contributions since the last rebase, including retracted values. The
//! operation count must upper-bound the additions used to build both the
//! component sum and its magnitude, including delta aggregation and merges.
//! The bound and state-integrity scheme must be reviewed before this is used
//! by the refresh path; these checks cannot detect every corrupted state.

use crate::error::PgTrickleError;
use pgrx::prelude::*;

const MAX_EXACT_COUNT: u64 = 1 << 53;
const MAX_CHANGES_SINCE_REBASE: u64 = 4096;
const MAX_VECTOR_DIMENSIONS: usize = 16_000;
const ABS_TOLERANCE: f64 = 1e-5;
const REL_TOLERANCE: f64 = 1e-6;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RebaseReason {
    InvalidState,
    ChangeLimit,
    NumericalError,
}

/// Internal initialization query for the admitted centroid shape.
#[pg_extern(schema = "pgtrickle", stable)]
fn _centroid_initialization_sql(query: &str) -> Option<String> {
    centroid_initialization_sql(query).unwrap_or_else(|error| pgrx::error!("{error}"))
}

/// Internal single-snapshot staging query for centroid initialization.
#[pg_extern(schema = "pgtrickle", stable)]
fn _centroid_staging_sql(query: &str) -> Option<String> {
    centroid_staging_sql(query).unwrap_or_else(|error| pgrx::error!("{error}"))
}

pub(crate) fn centroid_staging_sql(query: &str) -> Result<Option<String>, PgTrickleError> {
    let Some(initialization) = centroid_initialization_sql(query)? else {
        return Ok(None);
    };
    let row_id = crate::dvm::row_id_expr_for_query(query);
    if !row_id.starts_with("pgtrickle.encode_row_id_v3('GROUP_KEY',") {
        return Ok(None);
    }
    Ok(Some(format!(
        "SELECT {row_id} AS __pgt_row_id, sub.* FROM ({initialization}) sub"
    )))
}

pub(crate) fn centroid_initialization_sql(query: &str) -> Result<Option<String>, PgTrickleError> {
    if !crate::api::helpers::defining_query_is_immutable_without_sublinks(query)? {
        return Ok(None);
    }
    let mut result = crate::dvm::parse_defining_query_full(query)?;
    crate::dvm::parser::enrich_vector_aggregates(query, &mut result, false)?;
    let (columns, _) = crate::api::helpers::validate_defining_query(query)?;
    let crate::dvm::parser::OpTree::Aggregate {
        group_by,
        aggregates,
        ..
    } = &result.tree
    else {
        return Ok(None);
    };
    if aggregates.len() != 1 || columns.len() != group_by.len() + 1 {
        return Ok(None);
    }
    let mut expected = group_by
        .iter()
        .map(|expr| expr.output_name())
        .collect::<Vec<_>>();
    expected.push(aggregates[0].alias.clone());
    expected.sort_unstable();
    let mut actual = columns
        .into_iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    actual.sort_unstable();
    if expected != actual {
        return Ok(None);
    }
    let mut sources = result.tree.source_oids();
    sources.extend(result.cte_registry.source_oids());
    sources.sort_unstable();
    sources.dedup();
    if crate::cdc::first_rls_enabled_source(&sources)?.is_some() {
        return Ok(None);
    }
    if !crate::dvm::incremental_admission(&result, 'i', false)?.is_proven() {
        return Ok(None);
    }
    Ok(crate::dvm::operators::aggregate::centroid_initialization_sql(&result))
}

/// Add one signed vector contribution to a delta accumulator.
/// The layout is count, operation count, component sums, component magnitudes.
fn accumulate_delta(
    mut state: Vec<f64>,
    values: Option<&[f32]>,
    sign: i16,
) -> Result<Vec<f64>, PgTrickleError> {
    let invalid = |reason: &str| {
        PgTrickleError::InvalidArgument(format!("centroid delta accumulator: {reason}"))
    };
    if sign != 1 && sign != -1 {
        return Err(invalid("sign must be 1 or -1"));
    }
    if !state.is_empty()
        && (state.len() < 4
            || state.len() > 2 + MAX_VECTOR_DIMENSIONS * 2
            || !(state.len() - 2).is_multiple_of(2)
            || state.iter().any(|value| !value.is_finite())
            || state[0].fract() != 0.0
            || state[0].abs() > MAX_EXACT_COUNT as f64
            || state[1].fract() != 0.0
            || state[1] < state[0].abs()
            || state[1] > MAX_EXACT_COUNT as f64
            || state[2 + (state.len() - 2) / 2..]
                .iter()
                .any(|value| *value < 0.0))
    {
        return Err(invalid("invalid accumulator state"));
    }
    let Some(values) = values else {
        return Ok(state);
    };
    if values.is_empty() || values.len() > MAX_VECTOR_DIMENSIONS {
        return Err(invalid("invalid vector dimensions"));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(invalid("vector components must be finite"));
    }
    let dimensions = values.len();
    if state.is_empty() {
        state.resize(2 + dimensions * 2, 0.0);
    }
    if state.len() != 2 + dimensions * 2 {
        return Err(invalid(
            "invalid accumulator state or mismatched dimensions",
        ));
    }
    if state[1] >= MAX_EXACT_COUNT as f64
        || (sign > 0 && state[0] >= MAX_EXACT_COUNT as f64)
        || (sign < 0 && state[0] <= -(MAX_EXACT_COUNT as f64))
    {
        return Err(invalid("contribution count exceeds exact float64 range"));
    }
    state[0] += f64::from(sign);
    state[1] += 1.0;
    for (index, value) in values.iter().enumerate() {
        let value = f64::from(*value);
        state[2 + index] += f64::from(sign) * value;
        state[2 + dimensions + index] += value.abs();
        if !state[2 + index].is_finite() || !state[2 + dimensions + index].is_finite() {
            return Err(invalid("component accumulator overflow"));
        }
    }
    Ok(state)
}

/// Internal transition function for signed centroid deltas.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
fn _centroid_delta_step(
    state: Option<Array<f64>>,
    values: Option<Array<f32>>,
    sign: Option<i16>,
) -> Vec<f64> {
    let state = state
        .filter(|state| state.len() <= 2 + MAX_VECTOR_DIMENSIONS * 2)
        .and_then(|state| state.iter().collect::<Option<Vec<_>>>())
        .unwrap_or_else(|| pgrx::error!("centroid delta accumulator: NULL state or state element"));
    let sign = sign.unwrap_or_else(|| pgrx::error!("centroid delta accumulator: NULL sign"));
    let values = values.map(|values| {
        if values.len() > MAX_VECTOR_DIMENSIONS {
            pgrx::error!("centroid delta accumulator: invalid vector dimensions");
        }
        values
            .iter()
            .collect::<Option<Vec<_>>>()
            .unwrap_or_else(|| pgrx::error!("centroid delta accumulator: NULL vector component"))
    });
    accumulate_delta(state, values.as_deref(), sign).unwrap_or_else(|error| pgrx::error!("{error}"))
}

extension_sql!(
    r#"
CREATE AGGREGATE pgtrickle._centroid_delta(real[], smallint) (
    SFUNC = pgtrickle._centroid_delta_step,
    STYPE = double precision[],
    INITCOND = '{}',
    PARALLEL = UNSAFE
);
"#,
    name = "pg_trickle_centroid_delta",
    requires = [_centroid_delta_step],
);

/// Merge a signed delta with prior state without changing either input.
/// State layout is count, operations, changes, sums, magnitudes.
fn merge_delta(
    previous: &[f64],
    delta: &[f64],
    dimensions: usize,
    new_group: bool,
) -> Result<Vec<f64>, PgTrickleError> {
    let invalid =
        |reason: &str| PgTrickleError::InvalidArgument(format!("centroid state merge: {reason}"));
    if dimensions == 0 || dimensions > MAX_VECTOR_DIMENSIONS {
        return Err(invalid("invalid vector dimensions"));
    }
    if previous.is_empty() != new_group {
        return Err(invalid("missing state or unexpected state for group"));
    }
    let mut result = if previous.is_empty() {
        vec![0.0; 3 + 2 * dimensions]
    } else if previous.len() == 3 + 2 * dimensions {
        previous.to_vec()
    } else {
        return Err(invalid("invalid prior state dimensions"));
    };
    if result.iter().any(|value| !value.is_finite())
        || result[..3]
            .iter()
            .any(|value| *value < 0.0 || value.fract() != 0.0 || *value > MAX_EXACT_COUNT as f64)
        || result[1] < result[0]
        || result[1] < result[2]
        || result[3 + dimensions..].iter().any(|value| *value < 0.0)
        || result[3..3 + dimensions]
            .iter()
            .zip(&result[3 + dimensions..])
            .any(|(sum, magnitude)| sum.abs() > 2.0 * magnitude)
    {
        return Err(invalid("invalid prior state"));
    }
    if delta.is_empty() {
        return Ok(result);
    }
    if delta.len() != 2 + 2 * dimensions
        || delta.iter().any(|value| !value.is_finite())
        || delta[0].fract() != 0.0
        || delta[0].abs() > MAX_EXACT_COUNT as f64
        || delta[1].fract() != 0.0
        || delta[1] < delta[0].abs()
        || delta[1] > MAX_EXACT_COUNT as f64
        || (delta[1] == 0.0 && delta[2..].iter().any(|value| *value != 0.0))
        || delta[2 + dimensions..].iter().any(|value| *value < 0.0)
        || delta[2..2 + dimensions]
            .iter()
            .zip(&delta[2 + dimensions..])
            .any(|(sum, magnitude)| sum.abs() > 2.0 * magnitude)
    {
        return Err(invalid("invalid delta state"));
    }
    let count = result[0] as i64 + delta[0] as i64;
    let operations = (result[1] as u64)
        .checked_add(delta[1] as u64)
        .and_then(|value| value.checked_add(2))
        .ok_or_else(|| invalid("operation count overflow"))?;
    let changes = (result[2] as u64)
        .checked_add(delta[1] as u64)
        .ok_or_else(|| invalid("change count overflow"))?;
    if count < 0 || count as u64 > MAX_EXACT_COUNT || operations > MAX_EXACT_COUNT {
        return Err(invalid("count exceeds valid exact range"));
    }
    result[0] = count as f64;
    result[1] = operations as f64;
    result[2] = changes as f64;
    for index in 0..(2 * dimensions) {
        result[3 + index] += delta[2 + index];
        if !result[3 + index].is_finite() {
            return Err(invalid("component accumulator overflow"));
        }
    }
    Ok(result)
}

/// Initialize or rebase state from an insertion-only aggregate.
fn initialize_state(delta: &[f64], dimensions: usize) -> Result<Vec<f64>, PgTrickleError> {
    if !delta.is_empty() && (delta.len() < 2 || delta[0] != delta[1]) {
        return Err(PgTrickleError::InvalidArgument(
            "centroid state initialization requires insertion-only contributions".to_string(),
        ));
    }
    let mut state = merge_delta(&[], delta, dimensions, true)?;
    if !delta.is_empty() {
        state[1] = delta[1];
        state[2] = 0.0;
    }
    Ok(state)
}

/// Internal initialization of centroid state from a fresh group scan.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
fn _centroid_init_raw(delta: Option<Array<f64>>, dimensions: Option<i32>) -> Vec<f64> {
    let delta = delta
        .filter(|value| value.len() <= 2 + MAX_VECTOR_DIMENSIONS * 2)
        .and_then(|value| value.iter().collect::<Option<Vec<_>>>())
        .unwrap_or_else(|| pgrx::error!("centroid state initialization: invalid delta"));
    let dimensions = dimensions
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_else(|| {
            pgrx::error!("centroid state initialization: invalid vector dimensions")
        });
    initialize_state(&delta, dimensions).unwrap_or_else(|error| pgrx::error!("{error}"))
}

extension_sql!(
    r#"
CREATE FUNCTION pgtrickle._centroid_init(double precision[], integer)
RETURNS double precision[] LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT pgtrickle._centroid_init_raw(
        CASE WHEN $1 IS NULL OR (pg_catalog.cardinality($1) > 0 AND
                 (pg_catalog.array_ndims($1) IS DISTINCT FROM 1 OR pg_catalog.array_lower($1, 1) IS DISTINCT FROM 1))
             THEN NULL::double precision[] ELSE $1 END, $2)
$$;
"#,
    name = "pg_trickle_centroid_init",
    requires = [_centroid_init_raw],
);

/// Internal merge of a centroid state and its signed delta.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
fn _centroid_merge_raw(
    previous: Option<Array<f64>>,
    delta: Option<Array<f64>>,
    dimensions: Option<i32>,
    new_group: Option<bool>,
) -> Vec<f64> {
    let previous = previous
        .filter(|value| value.len() <= 3 + MAX_VECTOR_DIMENSIONS * 2)
        .and_then(|value| value.iter().collect::<Option<Vec<_>>>())
        .unwrap_or_else(|| pgrx::error!("centroid state merge: invalid prior state"));
    let delta = delta
        .filter(|value| value.len() <= 2 + MAX_VECTOR_DIMENSIONS * 2)
        .and_then(|value| value.iter().collect::<Option<Vec<_>>>())
        .unwrap_or_else(|| pgrx::error!("centroid state merge: invalid delta state"));
    let dimensions = dimensions
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_else(|| pgrx::error!("centroid state merge: invalid vector dimensions"));
    let new_group =
        new_group.unwrap_or_else(|| pgrx::error!("centroid state merge: missing group existence"));
    merge_delta(&previous, &delta, dimensions, new_group)
        .unwrap_or_else(|error| pgrx::error!("{error}"))
}

extension_sql!(
    r#"
CREATE FUNCTION pgtrickle._centroid_merge(double precision[], double precision[], integer, boolean)
RETURNS double precision[] LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT pgtrickle._centroid_merge_raw(
        CASE WHEN $1 IS NULL OR $2 IS NULL
                   OR (pg_catalog.cardinality($1) > 0 AND
                       (pg_catalog.array_ndims($1) IS DISTINCT FROM 1 OR pg_catalog.array_lower($1, 1) IS DISTINCT FROM 1))
                   OR (pg_catalog.cardinality($2) > 0 AND
                       (pg_catalog.array_ndims($2) IS DISTINCT FROM 1 OR pg_catalog.array_lower($2, 1) IS DISTINCT FROM 1))
             THEN NULL::double precision[] ELSE $1 END,
        $2, $3, $4)
$$;
"#,
    name = "pg_trickle_centroid_merge",
    requires = [_centroid_merge_raw],
);

/// Return float32 components only when the candidate passes the error guard.
fn finalize_state(state: &[f64], dimensions: usize) -> Result<Option<Vec<f32>>, PgTrickleError> {
    if state.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "centroid finalization requires initialized state".to_string(),
        ));
    }
    let state = merge_delta(state, &[], dimensions, false)?;
    let count = state[0] as u64;
    let reason = rebase_reason(
        count,
        state[1] as u64,
        state[2] as u64,
        &state[3..3 + dimensions],
        &state[3 + dimensions..],
    );
    if let Some(reason) = reason {
        return Err(PgTrickleError::InvalidArgument(format!(
            "centroid state requires rebase: {reason:?}"
        )));
    }
    if count == 0 {
        return Ok(None);
    }
    Ok(Some(
        state[3..3 + dimensions]
            .iter()
            .map(|sum| (*sum / count as f64) as f32)
            .collect(),
    ))
}

/// Reject a candidate whose non-null count cannot fit within its group.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
fn _centroid_check_count(
    state: Option<Array<f64>>,
    group_count: Option<i64>,
    dimensions: Option<i32>,
) -> Vec<f64> {
    let state = state
        .filter(|value| value.len() <= 3 + MAX_VECTOR_DIMENSIONS * 2)
        .and_then(|value| value.iter().collect::<Option<Vec<_>>>())
        .unwrap_or_else(|| pgrx::error!("centroid group count: invalid state"));
    let dimensions = dimensions
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_else(|| pgrx::error!("centroid group count: invalid dimensions"));
    let count = group_count
        .filter(|value| *value >= 0)
        .unwrap_or_else(|| pgrx::error!("centroid group count: negative or missing group count"));
    let state =
        merge_delta(&state, &[], dimensions, false).unwrap_or_else(|error| pgrx::error!("{error}"));
    if state[0] > count as f64 {
        pgrx::error!("centroid group count: non-null count exceeds group count");
    }
    state
}

/// Internal finalization of a numerically qualified centroid state.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
fn _centroid_finalize_raw(state: Option<Array<f64>>, dimensions: Option<i32>) -> Option<Vec<f32>> {
    let state = state
        .filter(|value| value.len() <= 3 + MAX_VECTOR_DIMENSIONS * 2)
        .and_then(|value| value.iter().collect::<Option<Vec<_>>>())
        .unwrap_or_else(|| pgrx::error!("centroid finalization: invalid state"));
    let dimensions = dimensions
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_else(|| pgrx::error!("centroid finalization: invalid vector dimensions"));
    finalize_state(&state, dimensions).unwrap_or_else(|error| pgrx::error!("{error}"))
}

extension_sql!(
    r#"
CREATE FUNCTION pgtrickle._centroid_finalize(double precision[], integer)
RETURNS real[] LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT pgtrickle._centroid_finalize_raw(
        CASE WHEN $1 IS NULL OR pg_catalog.array_ndims($1) IS DISTINCT FROM 1
                   OR pg_catalog.array_lower($1, 1) IS DISTINCT FROM 1
             THEN NULL::double precision[] ELSE $1 END, $2)
$$;
"#,
    name = "pg_trickle_centroid_finalize",
    requires = [_centroid_finalize_raw],
);

/// Internal numerical guard for a candidate centroid state.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
fn _centroid_rebase_reason_raw(
    count: Option<i64>,
    operations: Option<i64>,
    changes: Option<i64>,
    sums: Option<Array<f64>>,
    magnitudes: Option<Array<f64>>,
) -> Option<&'static str> {
    let invalid = Some("INVALID_STATE");
    let (Some(count), Some(operations), Some(changes), Some(sums), Some(magnitudes)) =
        (count, operations, changes, sums, magnitudes)
    else {
        return invalid;
    };
    let (Ok(count), Ok(operations), Ok(changes)) = (
        u64::try_from(count),
        u64::try_from(operations),
        u64::try_from(changes),
    ) else {
        return invalid;
    };
    if sums.len() > MAX_VECTOR_DIMENSIONS || magnitudes.len() > MAX_VECTOR_DIMENSIONS {
        return invalid;
    }
    let Some(sums): Option<Vec<f64>> = sums.iter().collect() else {
        return invalid;
    };
    let Some(magnitudes): Option<Vec<f64>> = magnitudes.iter().collect() else {
        return invalid;
    };
    match rebase_reason(count, operations, changes, &sums, &magnitudes) {
        None => None,
        Some(RebaseReason::InvalidState) => invalid,
        Some(RebaseReason::ChangeLimit) => Some("CHANGE_LIMIT"),
        Some(RebaseReason::NumericalError) => Some("NUMERICAL_ERROR"),
    }
}

extension_sql!(
    r#"
CREATE FUNCTION pgtrickle._centroid_rebase_reason(
    bigint, bigint, bigint, double precision[], double precision[]
) RETURNS text LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN pg_catalog.array_ndims($4) IS DISTINCT FROM 1
                      OR pg_catalog.array_ndims($5) IS DISTINCT FROM 1
                      OR pg_catalog.array_lower($4, 1) IS DISTINCT FROM 1
                      OR pg_catalog.array_lower($5, 1) IS DISTINCT FROM 1
                THEN 'INVALID_STATE'
                ELSE pgtrickle._centroid_rebase_reason_raw($1, $2, $3, $4, $5)
           END
$$;
"#,
    name = "pg_trickle_centroid_rebase_guard",
    requires = [_centroid_rebase_reason_raw],
);

/// Decide whether every component can meet the error tolerance against a
/// fresh pgvector AVG, irrespective of the native aggregation order.
pub(crate) fn rebase_reason(
    count: u64,
    operations: u64,
    changes: u64,
    sums: &[f64],
    magnitudes: &[f64],
) -> Option<RebaseReason> {
    if sums.is_empty()
        || sums.len() > MAX_VECTOR_DIMENSIONS
        || sums.len() != magnitudes.len()
        || count > MAX_EXACT_COUNT
        || operations < count
        || operations < changes
        || operations > MAX_EXACT_COUNT
        || sums.iter().zip(magnitudes).any(|(&sum, &magnitude)| {
            !sum.is_finite()
                || !magnitude.is_finite()
                || magnitude < 0.0
                || sum.abs() > 2.0 * magnitude
        })
    {
        return Some(RebaseReason::InvalidState);
    }
    if changes >= MAX_CHANGES_SINCE_REBASE {
        return Some(RebaseReason::ChangeLimit);
    }
    // Two float64 sums (the maintained state and a fresh native aggregate)
    // are bounded using a deliberately loose multiple of gamma_n. The
    // operation cap keeps each gamma denominator well away from zero and
    // covers rounding in the nonnegative magnitude accumulator as well.
    let steps = operations as f64 + count as f64 + 2.0;
    if steps * f64::EPSILON >= 0.25 {
        return Some(RebaseReason::NumericalError);
    }
    if count == 0 {
        // Retractions can leave rounding residue after the last non-NULL input.
        return if sums.iter().zip(magnitudes).all(|(&sum, &magnitude)| {
            let bound = 8.0 * f64::EPSILON * steps * magnitude;
            bound.is_finite() && sum.abs() <= bound
        }) {
            None
        } else {
            Some(RebaseReason::InvalidState)
        };
    }
    for (&sum, &magnitude) in sums.iter().zip(magnitudes) {
        let average = sum / count as f64;
        let accumulation_error = 8.0 * f64::EPSILON * steps * magnitude / count as f64;
        // Both results are rounded to vector's float32 components.
        let conversion_error = 2.0 * f64::from(f32::EPSILON) * (average.abs() + accumulation_error)
            + 2.0 * f64::from(f32::from_bits(1));
        let error = accumulation_error + conversion_error;
        let tolerance = ABS_TOLERANCE + REL_TOLERANCE * (average.abs() - error).max(0.0);
        if !error.is_finite() || average.abs() + error > f64::from(f32::MAX) || error > tolerance {
            return Some(RebaseReason::NumericalError);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulate_delta_preserves_duplicates_nulls_and_deletes() {
        let state = accumulate_delta(Vec::new(), Some(&[1.0, 2.0]), 1).unwrap();
        let state = accumulate_delta(state, Some(&[1.0, 2.0]), 1).unwrap();
        let state = accumulate_delta(state, None, -1).unwrap();
        let state = accumulate_delta(state, Some(&[1.0, 2.0]), -1).unwrap();
        assert_eq!(state, vec![1.0, 3.0, 1.0, 2.0, 3.0, 6.0]);
    }

    #[test]
    fn test_accumulate_delta_rejects_invalid_and_overflowing_state() {
        assert!(accumulate_delta(vec![0.0, 0.0, 0.0], None, 1).is_err());
        assert!(accumulate_delta(Vec::new(), Some(&[f32::NAN]), 1).is_err());
        assert!(accumulate_delta(Vec::new(), Some(&[1.0]), 0).is_err());
        assert!(accumulate_delta(vec![0.0, 0.0, 0.0, 0.0], Some(&[1.0, 2.0]), 1).is_err());
        assert!(
            accumulate_delta(
                vec![MAX_EXACT_COUNT as f64, MAX_EXACT_COUNT as f64, 0.0, 0.0],
                Some(&[1.0]),
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn test_merge_delta_tracks_contributions_and_empty_groups() {
        let delta = vec![1.0, 3.0, 1.0, 2.0, 3.0, 6.0];
        let state = merge_delta(&[], &delta, 2, true).unwrap();
        assert_eq!(state, vec![1.0, 5.0, 3.0, 1.0, 2.0, 3.0, 6.0]);
        assert_eq!(merge_delta(&state, &[], 2, false).unwrap(), state);
        let deleted = vec![-1.0, 1.0, -1.0, -2.0, 1.0, 2.0];
        let state = merge_delta(&state, &deleted, 2, false).unwrap();
        assert_eq!(state, vec![0.0, 8.0, 4.0, 0.0, 0.0, 4.0, 8.0]);
        assert_eq!(rebase_reason(0, 8, 4, &state[3..5], &state[5..]), None);
        assert_eq!(merge_delta(&[], &[], 2, true).unwrap(), vec![0.0; 7]);
    }

    #[test]
    fn test_initialize_state_resets_change_budget_and_rejects_retractions() {
        let state = initialize_state(&[5000.0, 5000.0, 5000.0, 5000.0], 1).unwrap();
        assert_eq!(state, vec![5000.0, 5000.0, 0.0, 5000.0, 5000.0]);
        assert_eq!(
            rebase_reason(5000, 5000, 0, &state[3..4], &state[4..]),
            None
        );
        assert_eq!(initialize_state(&[], 1).unwrap(), vec![0.0; 5]);
        assert!(initialize_state(&[1.0, 3.0, 1.0, 3.0], 1).is_err());
    }

    #[test]
    fn test_finalize_state_returns_mean_or_requires_rebase() {
        let state = initialize_state(&[2.0, 2.0, 3.0, 4.0, 3.0, 4.0], 2).unwrap();
        assert_eq!(finalize_state(&state, 2).unwrap(), Some(vec![1.5, 2.0]));
        assert_eq!(finalize_state(&[0.0; 5], 1).unwrap(), None);
        assert!(finalize_state(&[1.0, 3.0, 1.0, 0.0, 2e20], 1).is_err());
        assert!(finalize_state(&[], 1).is_err());
    }

    #[test]
    fn test_merge_delta_rejects_corrupt_state_and_count_underflow() {
        assert!(merge_delta(&[], &[-1.0, 1.0, -1.0, 1.0], 1, true).is_err());
        assert!(merge_delta(&[1.0, 0.0, 0.0, 1.0, 1.0], &[], 1, false).is_err());
        assert!(merge_delta(&[], &[1.0, 1.0, 1.0], 1, true).is_err());
        assert!(merge_delta(&[], &[0.0, 0.0, 1.0, 1.0], 1, true).is_err());
        assert!(merge_delta(&[], &[], 0, true).is_err());
        assert!(merge_delta(&[], &[], 1, false).is_err());
        assert!(merge_delta(&[0.0; 5], &[], 1, true).is_err());
    }

    #[test]
    fn test_rebase_reason_rejects_cancellation_and_invalid_state() {
        assert_eq!(
            rebase_reason(1, 3, 1, &[0.0], &[2e20]),
            Some(RebaseReason::NumericalError)
        );
        assert_eq!(
            rebase_reason(1, 1, 0, &[1.0], &[]),
            Some(RebaseReason::InvalidState)
        );
        assert_eq!(
            rebase_reason(1, 1, 0, &[f64::NAN], &[1.0]),
            Some(RebaseReason::InvalidState)
        );
        assert_eq!(
            rebase_reason(1, 1, 0, &[1.0], &[0.0]),
            Some(RebaseReason::InvalidState)
        );
        assert_eq!(
            rebase_reason(
                1,
                MAX_CHANGES_SINCE_REBASE,
                MAX_CHANGES_SINCE_REBASE,
                &[1.0],
                &[1.0]
            ),
            Some(RebaseReason::ChangeLimit)
        );
        assert_eq!(rebase_reason(2, 2, 0, &[5.0], &[5.0]), None);
    }

    #[test]
    fn test_empty_centroid_allows_only_bounded_rounding_residue() {
        assert_eq!(rebase_reason(0, 1226, 428, &[1.3e-11], &[1000.0]), None);
        assert_eq!(
            rebase_reason(0, 1226, 428, &[1.0], &[1000.0]),
            Some(RebaseReason::InvalidState)
        );
        assert_eq!(
            rebase_reason(0, 1, 1, &[1e-20], &[0.0]),
            Some(RebaseReason::InvalidState)
        );
        assert_eq!(
            rebase_reason(0, 1 << 52, 1, &[0.0], &[1.0]),
            Some(RebaseReason::NumericalError)
        );
    }

    #[test]
    fn test_empty_centroid_residue_cannot_bypass_later_error_guard() {
        let mut initial = Vec::new();
        for value in [1e20_f32, 1.0, -1e20] {
            initial = accumulate_delta(initial, Some(&[value]), 1).unwrap();
        }
        let state = initialize_state(&initial, 1).unwrap();
        let mut deleted = Vec::new();
        for value in [1e20_f32, -1e20, 1.0] {
            deleted = accumulate_delta(deleted, Some(&[value]), -1).unwrap();
        }
        let empty = merge_delta(&state, &deleted, 1, false).unwrap();
        assert_eq!(empty[0], 0.0);
        assert_ne!(empty[3], 0.0);
        assert_eq!(finalize_state(&empty, 1).unwrap(), None);
        let added = accumulate_delta(Vec::new(), Some(&[2.0]), 1).unwrap();
        let nonempty = merge_delta(&empty, &added, 1, false).unwrap();
        assert!(finalize_state(&nonempty, 1).is_err());
    }

    #[test]
    fn test_rebase_reason_bounds_repeated_insertions_and_retractions() {
        let mut values: Vec<f32> = Vec::new();
        let (mut sum, mut magnitude) = (0.0_f64, 0.0_f64);
        let (mut operations, mut changes) = (0_u64, 0_u64);
        let mut seed = 17_u64;
        let (mut accepted, mut rebased) = (0, 0);
        for _ in 0..20_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            if !values.is_empty() && ((seed >> 20) & 1 == 0 || values.len() >= 64) {
                let index = ((seed >> 16) as usize) % values.len();
                let value = values.swap_remove(index);
                sum -= f64::from(value);
                magnitude += f64::from(value).abs();
            } else {
                let value = if seed.is_multiple_of(97) {
                    1e20_f32
                } else {
                    ((seed >> 24) % 2001) as f32 - 1000.0
                };
                values.push(value);
                sum += f64::from(value);
                magnitude += f64::from(value).abs();
            }
            operations += 1;
            changes += 1;
            let count = values.len() as u64;
            if rebase_reason(count, operations, changes, &[sum], &[magnitude]).is_some() {
                rebased += 1;
                sum = values.iter().map(|value| f64::from(*value)).sum();
                magnitude = values.iter().map(|value| f64::from(*value).abs()).sum();
                operations = count;
                changes = 0;
                continue;
            }
            if count > 0 {
                accepted += 1;
                let native = (values.iter().map(|value| f64::from(*value)).sum::<f64>()
                    / count as f64) as f32;
                let maintained = (sum / count as f64) as f32;
                let error = (f64::from(native) - f64::from(maintained)).abs();
                assert!(error <= ABS_TOLERANCE + REL_TOLERANCE * f64::from(native).abs());
            }
        }
        assert!(accepted > 1000 && rebased > 0);
    }
}
