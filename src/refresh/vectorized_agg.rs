//! Bounded SPI execution for MT-8 aggregate plans.

use std::collections::HashMap;
use std::time::Duration;

#[cfg(not(test))]
use std::time::Instant;

#[cfg(not(test))]
use pgrx::prelude::*;

use crate::dvm::operators::vectorized_agg::{
    VectorAggregateExprPlan, VectorAggregateFunction, VectorAggregatePlan, VectorType,
};
use crate::error::PgTrickleError;

pub const VECTOR_PAGE_ROWS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScalarValue {
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Date(i32),
    Timestamp(i64),
    TimestampTz(i64),
}

/// One dependency-free, owned input column. `nulls[i]` describes `values[i]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedColumn {
    Bool(Vec<bool>, Vec<bool>),
    Int2(Vec<i16>, Vec<bool>),
    Int4(Vec<i32>, Vec<bool>),
    Int8(Vec<i64>, Vec<bool>),
    Date(Vec<i32>, Vec<bool>),
    Timestamp(Vec<i64>, Vec<bool>),
    TimestampTz(Vec<i64>, Vec<bool>),
}

impl OwnedColumn {
    fn with_capacity(value_type: VectorType, capacity: usize) -> Self {
        match value_type {
            VectorType::Bool => {
                Self::Bool(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            VectorType::Int2 => {
                Self::Int2(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            VectorType::Int4 => {
                Self::Int4(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            VectorType::Int8 => {
                Self::Int8(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            VectorType::Date => {
                Self::Date(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            VectorType::Timestamp => {
                Self::Timestamp(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            VectorType::TimestampTz => {
                Self::TimestampTz(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Bool(_, nulls)
            | Self::Int2(_, nulls)
            | Self::Int4(_, nulls)
            | Self::Int8(_, nulls)
            | Self::Date(_, nulls)
            | Self::Timestamp(_, nulls)
            | Self::TimestampTz(_, nulls) => nulls.len(),
        }
    }

    fn value(&self, index: usize) -> Option<ScalarValue> {
        match self {
            Self::Bool(values, nulls) => (!nulls[index]).then(|| ScalarValue::Bool(values[index])),
            Self::Int2(values, nulls) => (!nulls[index]).then(|| ScalarValue::Int2(values[index])),
            Self::Int4(values, nulls) => (!nulls[index]).then(|| ScalarValue::Int4(values[index])),
            Self::Int8(values, nulls) => (!nulls[index]).then(|| ScalarValue::Int8(values[index])),
            Self::Date(values, nulls) => (!nulls[index]).then(|| ScalarValue::Date(values[index])),
            Self::Timestamp(values, nulls) => {
                (!nulls[index]).then(|| ScalarValue::Timestamp(values[index]))
            }
            Self::TimestampTz(values, nulls) => {
                (!nulls[index]).then(|| ScalarValue::TimestampTz(values[index]))
            }
        }
    }

    fn byte_width(&self) -> usize {
        match self {
            Self::Bool(..) => 2,
            Self::Int2(..) => 3,
            Self::Int4(..) | Self::Date(..) => 5,
            Self::Int8(..) | Self::Timestamp(..) | Self::TimestampTz(..) => 9,
        }
    }

    #[cfg(not(test))]
    fn push_datum(&mut self, datum: Option<pg_sys::Datum>) {
        let is_null = datum.is_none();
        let raw = datum.map(pg_sys::Datum::value).unwrap_or_default();
        match self {
            Self::Bool(values, nulls) => {
                values.push(raw != 0);
                nulls.push(is_null);
            }
            Self::Int2(values, nulls) => {
                values.push(raw as i16);
                nulls.push(is_null);
            }
            Self::Int4(values, nulls) => {
                values.push(raw as i32);
                nulls.push(is_null);
            }
            Self::Int8(values, nulls) => {
                values.push(raw as i64);
                nulls.push(is_null);
            }
            Self::Date(values, nulls) => {
                values.push(raw as i32);
                nulls.push(is_null);
            }
            Self::Timestamp(values, nulls) => {
                values.push(raw as i64);
                nulls.push(is_null);
            }
            Self::TimestampTz(values, nulls) => {
                values.push(raw as i64);
                nulls.push(is_null);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPage {
    pub actions: Vec<i8>,
    pub group_columns: Vec<OwnedColumn>,
    /// One entry per aggregate. COUNT(*) has `None`.
    pub aggregate_columns: Vec<Option<OwnedColumn>>,
}

impl VectorPage {
    pub fn bytes(&self) -> usize {
        self.actions.len()
            + self
                .group_columns
                .iter()
                .chain(self.aggregate_columns.iter().flatten())
                .map(|column| column.len() * column.byte_width())
                .sum::<usize>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateTransition {
    Count {
        inserted: i64,
        deleted: i64,
    },
    Sum {
        inserted: i64,
        deleted: i64,
        inserted_nonnull: i64,
        deleted_nonnull: i64,
    },
    Avg {
        inserted_sum: i64,
        deleted_sum: i64,
        inserted_count: i64,
        deleted_count: i64,
    },
    MinMax {
        inserted: Option<ScalarValue>,
        deleted: Option<ScalarValue>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducedGroup {
    pub key: Vec<Option<ScalarValue>>,
    pub inserted_rows: i64,
    pub deleted_rows: i64,
    pub transitions: Vec<AggregateTransition>,
}

pub fn reduce_page(
    plan: &VectorAggregatePlan,
    page: &VectorPage,
) -> Result<Vec<ReducedGroup>, PgTrickleError> {
    validate_page(plan, page)?;
    let mut indexes = HashMap::<Vec<Option<ScalarValue>>, usize>::new();
    let mut groups = Vec::<ReducedGroup>::new();

    for row in 0..page.actions.len() {
        let key = page
            .group_columns
            .iter()
            .map(|column| column.value(row))
            .collect::<Vec<_>>();
        let group_index = match indexes.get(&key) {
            Some(index) => *index,
            None => {
                let index = groups.len();
                groups.push(ReducedGroup {
                    key: key.clone(),
                    inserted_rows: 0,
                    deleted_rows: 0,
                    transitions: plan.aggregates.iter().map(empty_transition).collect(),
                });
                indexes.insert(key, index);
                index
            }
        };
        let group = &mut groups[group_index];
        let inserted = page.actions[row] == 1;
        let row_counter = if inserted {
            &mut group.inserted_rows
        } else {
            &mut group.deleted_rows
        };
        *row_counter = checked_add(*row_counter, 1, "COUNT(*)")?;

        for (index, aggregate) in plan.aggregates.iter().enumerate() {
            let value = page.aggregate_columns[index]
                .as_ref()
                .and_then(|column| column.value(row));
            accumulate(&mut group.transitions[index], aggregate, value, inserted)?;
        }
    }
    Ok(groups)
}

fn combine_group(
    plan: &VectorAggregatePlan,
    target: &mut ReducedGroup,
    incoming: &ReducedGroup,
) -> Result<(), PgTrickleError> {
    target.inserted_rows = checked_add(target.inserted_rows, incoming.inserted_rows, "COUNT(*)")?;
    target.deleted_rows = checked_add(target.deleted_rows, incoming.deleted_rows, "COUNT(*)")?;
    for ((target, incoming), aggregate) in target
        .transitions
        .iter_mut()
        .zip(&incoming.transitions)
        .zip(&plan.aggregates)
    {
        match (target, incoming) {
            (
                AggregateTransition::Count { inserted, deleted },
                AggregateTransition::Count {
                    inserted: add_inserted,
                    deleted: add_deleted,
                },
            ) => {
                *inserted = checked_add(*inserted, *add_inserted, &aggregate.output_alias)?;
                *deleted = checked_add(*deleted, *add_deleted, &aggregate.output_alias)?;
            }
            (
                AggregateTransition::Sum {
                    inserted,
                    deleted,
                    inserted_nonnull,
                    deleted_nonnull,
                },
                AggregateTransition::Sum {
                    inserted: add_inserted,
                    deleted: add_deleted,
                    inserted_nonnull: add_inserted_nonnull,
                    deleted_nonnull: add_deleted_nonnull,
                },
            ) => {
                *inserted = checked_add(*inserted, *add_inserted, &aggregate.output_alias)?;
                *deleted = checked_add(*deleted, *add_deleted, &aggregate.output_alias)?;
                *inserted_nonnull = checked_add(
                    *inserted_nonnull,
                    *add_inserted_nonnull,
                    &aggregate.output_alias,
                )?;
                *deleted_nonnull = checked_add(
                    *deleted_nonnull,
                    *add_deleted_nonnull,
                    &aggregate.output_alias,
                )?;
            }
            (
                AggregateTransition::Avg {
                    inserted_sum,
                    deleted_sum,
                    inserted_count,
                    deleted_count,
                },
                AggregateTransition::Avg {
                    inserted_sum: add_inserted_sum,
                    deleted_sum: add_deleted_sum,
                    inserted_count: add_inserted_count,
                    deleted_count: add_deleted_count,
                },
            ) => {
                *inserted_sum =
                    checked_add(*inserted_sum, *add_inserted_sum, &aggregate.output_alias)?;
                *deleted_sum =
                    checked_add(*deleted_sum, *add_deleted_sum, &aggregate.output_alias)?;
                *inserted_count = checked_add(
                    *inserted_count,
                    *add_inserted_count,
                    &aggregate.output_alias,
                )?;
                *deleted_count =
                    checked_add(*deleted_count, *add_deleted_count, &aggregate.output_alias)?;
            }
            (
                AggregateTransition::MinMax { inserted, deleted },
                AggregateTransition::MinMax {
                    inserted: add_inserted,
                    deleted: add_deleted,
                },
            ) => {
                merge_extreme(inserted, *add_inserted, aggregate.function);
                merge_extreme(deleted, *add_deleted, aggregate.function);
            }
            _ => {
                return Err(PgTrickleError::InternalError(
                    "vector aggregate transition shape changed".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn merge_extreme(
    current: &mut Option<ScalarValue>,
    incoming: Option<ScalarValue>,
    function: VectorAggregateFunction,
) {
    if let Some(value) = incoming {
        let replace = current.is_none_or(|existing| match function {
            VectorAggregateFunction::Min => value < existing,
            VectorAggregateFunction::Max => value > existing,
            _ => false,
        });
        if replace {
            *current = Some(value);
        }
    }
}

fn aggregate_group_limit(plan: &VectorAggregatePlan, byte_limit: u64) -> usize {
    // ponytail: conservative HashMap estimate; add allocator accounting if the
    // fixed-width aggregate matrix grows beyond these entry shapes.
    let bytes_per_group = 128usize
        .saturating_add(plan.group_keys.len().saturating_mul(32))
        .saturating_add(plan.aggregates.len().saturating_mul(64));
    ((byte_limit as usize / 2) / bytes_per_group.max(1)).max(1)
}

fn validate_page(plan: &VectorAggregatePlan, page: &VectorPage) -> Result<(), PgTrickleError> {
    let rows = page.actions.len();
    if rows > VECTOR_PAGE_ROWS
        || page.group_columns.len() != plan.group_keys.len()
        || page.aggregate_columns.len() != plan.aggregates.len()
        || page.group_columns.iter().any(|column| column.len() != rows)
        || page
            .aggregate_columns
            .iter()
            .flatten()
            .any(|column| column.len() != rows)
        || page.actions.iter().any(|action| !matches!(action, -1 | 1))
    {
        return Err(PgTrickleError::InternalError(
            "invalid vector aggregate page shape".to_string(),
        ));
    }
    for (aggregate, column) in plan.aggregates.iter().zip(&page.aggregate_columns) {
        if aggregate.input.is_some() != column.is_some() {
            return Err(PgTrickleError::InternalError(
                "vector aggregate input column mismatch".to_string(),
            ));
        }
    }
    Ok(())
}

fn empty_transition(plan: &VectorAggregateExprPlan) -> AggregateTransition {
    match plan.function {
        VectorAggregateFunction::CountStar | VectorAggregateFunction::Count => {
            AggregateTransition::Count {
                inserted: 0,
                deleted: 0,
            }
        }
        VectorAggregateFunction::Sum => AggregateTransition::Sum {
            inserted: 0,
            deleted: 0,
            inserted_nonnull: 0,
            deleted_nonnull: 0,
        },
        VectorAggregateFunction::Avg => AggregateTransition::Avg {
            inserted_sum: 0,
            deleted_sum: 0,
            inserted_count: 0,
            deleted_count: 0,
        },
        VectorAggregateFunction::Min | VectorAggregateFunction::Max => {
            AggregateTransition::MinMax {
                inserted: None,
                deleted: None,
            }
        }
    }
}

fn accumulate(
    transition: &mut AggregateTransition,
    plan: &VectorAggregateExprPlan,
    value: Option<ScalarValue>,
    inserted: bool,
) -> Result<(), PgTrickleError> {
    match transition {
        AggregateTransition::Count {
            inserted: insert_count,
            deleted: delete_count,
        } => {
            if matches!(plan.function, VectorAggregateFunction::CountStar) || value.is_some() {
                let count = if inserted { insert_count } else { delete_count };
                *count = checked_add(*count, 1, &plan.output_alias)?;
            }
        }
        AggregateTransition::Sum {
            inserted: insert_sum,
            deleted: delete_sum,
            inserted_nonnull,
            deleted_nonnull,
        } => {
            if let Some(value) = value {
                let value = integer_value(value, &plan.output_alias)?;
                let (sum, count) = if inserted {
                    (insert_sum, inserted_nonnull)
                } else {
                    (delete_sum, deleted_nonnull)
                };
                *sum = checked_add(*sum, value, &plan.output_alias)?;
                *count = checked_add(*count, 1, &plan.output_alias)?;
            }
        }
        AggregateTransition::Avg {
            inserted_sum,
            deleted_sum,
            inserted_count,
            deleted_count,
        } => {
            if let Some(value) = value {
                let value = integer_value(value, &plan.output_alias)?;
                let (sum, count) = if inserted {
                    (inserted_sum, inserted_count)
                } else {
                    (deleted_sum, deleted_count)
                };
                *sum = checked_add(*sum, value, &plan.output_alias)?;
                *count = checked_add(*count, 1, &plan.output_alias)?;
            }
        }
        AggregateTransition::MinMax {
            inserted: insert_extreme,
            deleted: delete_extreme,
        } => {
            if let Some(value) = value {
                let extreme = if inserted {
                    insert_extreme
                } else {
                    delete_extreme
                };
                let replace = extreme.is_none_or(|current| match plan.function {
                    VectorAggregateFunction::Min => value < current,
                    VectorAggregateFunction::Max => value > current,
                    _ => false,
                });
                if replace {
                    *extreme = Some(value);
                }
            }
        }
    }
    Ok(())
}

fn integer_value(value: ScalarValue, aggregate: &str) -> Result<i64, PgTrickleError> {
    match value {
        ScalarValue::Int2(value) => Ok(i64::from(value)),
        ScalarValue::Int4(value) => Ok(i64::from(value)),
        _ => Err(PgTrickleError::InternalError(format!(
            "non-integer input reached vector aggregate '{aggregate}'"
        ))),
    }
}

fn checked_add(left: i64, right: i64, aggregate: &str) -> Result<i64, PgTrickleError> {
    left.checked_add(right)
        .ok_or_else(|| PgTrickleError::VectorAggregateOverflow {
            aggregate: aggregate.to_string(),
        })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorAggregateStats {
    pub rows_read: u64,
    pub pages_completed: u64,
    pub groups_emitted: u64,
    pub groups_rescanned: u64,
    pub bytes_copied: u64,
    pub largest_page_bytes: u64,
    pub applied_rows: u64,
    pub read_time: Duration,
    pub reduce_time: Duration,
    pub rescan_time: Duration,
    pub apply_time: Duration,
}

/// Existing relations and SQL shells supplied by the refresh orchestrator.
/// Every statement runs in the caller's current transaction.
pub struct VectorAggregateExecution<'a> {
    pub accumulator_relation: &'a str,
    pub delta_relation: &'a str,
    /// Caller proved that a MIN/MAX window has no delete rows.
    pub insert_only_minmax: bool,
    /// Converts accumulator rows to the type-identical logical delta relation.
    pub finalize_page_sql: &'a str,
    /// Existing DELETE, UPDATE, INSERT or MERGE apply statements.
    pub apply_page_sql: &'a [&'a str],
    pub page_byte_limit: u64,
}

#[cfg(not(test))]
pub fn execute_vectorized_aggregate(
    plan: &VectorAggregatePlan,
    execution: &VectorAggregateExecution<'_>,
) -> Result<VectorAggregateStats, PgTrickleError> {
    let fetch_rows = page_row_limit(plan, execution.page_byte_limit);
    let query = source_query(plan)?;
    let cursor_name = Spi::connect(|client| {
        client
            .try_open_cursor(
                &query,
                &[
                    plan.frontier_placeholders.0.as_str().into(),
                    plan.frontier_placeholders.1.as_str().into(),
                ],
            )
            .map(|cursor| cursor.detach_into_name())
    })
    .map_err(|error| PgTrickleError::SpiError(format!("vector cursor open: {error}")))?;

    let mut stats = VectorAggregateStats::default();
    let mut accumulated = HashMap::<Vec<Option<ScalarValue>>, ReducedGroup>::new();
    let group_limit = aggregate_group_limit(plan, execution.page_byte_limit);
    loop {
        let read_started = Instant::now();
        let page = fetch_page(&cursor_name, plan, fetch_rows)?;
        stats.read_time += read_started.elapsed();
        let Some(page) = page else { break };
        stats.rows_read = checked_stat_add(stats.rows_read, page.actions.len() as u64)?;
        let page_bytes = page.bytes() as u64;
        stats.bytes_copied = checked_stat_add(stats.bytes_copied, page_bytes)?;
        stats.largest_page_bytes = stats.largest_page_bytes.max(page_bytes);

        let reduce_started = Instant::now();
        let groups = reduce_page(plan, &page)?;
        for mut group in groups {
            let key = std::mem::take(&mut group.key);
            if !accumulated.contains_key(&key) && accumulated.len() >= group_limit {
                stats.groups_emitted = checked_stat_add(
                    stats.groups_emitted,
                    flush_groups(plan, execution.accumulator_relation, &mut accumulated)?,
                )?;
            }
            if let Some(existing) = accumulated.get_mut(&key) {
                combine_group(plan, existing, &group)?;
            } else {
                accumulated.insert(key, group);
            }
        }
        stats.reduce_time += reduce_started.elapsed();

        stats.pages_completed = checked_stat_add(stats.pages_completed, 1)?;
    }
    if stats.pages_completed > 0 {
        stats.groups_emitted = checked_stat_add(
            stats.groups_emitted,
            flush_groups(plan, execution.accumulator_relation, &mut accumulated)?,
        )?;
        let apply_started = Instant::now();
        Spi::run(execution.finalize_page_sql)
            .map_err(|error| PgTrickleError::SpiError(format!("vector finalize: {error}")))?;
        for sql in execution.apply_page_sql {
            let applied = Spi::connect_mut(|client| {
                client
                    .update(*sql, None, &[])
                    .map(|table| table.len() as u64)
                    .map_err(|error| PgTrickleError::SpiError(format!("vector apply: {error}")))
            })?;
            stats.applied_rows = checked_stat_add(stats.applied_rows, applied)?;
        }
        truncate_relation(execution.accumulator_relation)?;
        truncate_relation(execution.delta_relation)?;
        stats.apply_time += apply_started.elapsed();
    }
    Ok(stats)
}

#[cfg(not(test))]
fn flush_groups(
    plan: &VectorAggregatePlan,
    relation_name: &str,
    groups: &mut HashMap<Vec<Option<ScalarValue>>, ReducedGroup>,
) -> Result<u64, PgTrickleError> {
    let emitted = groups.len() as u64;
    let mut rows = Vec::with_capacity(groups.len());
    for (key, mut group) in groups.drain() {
        group.key = key;
        rows.push(group);
    }
    insert_groups(plan, relation_name, &rows)?;
    Ok(emitted)
}

/// Type-only SELECT used with `prepare_owner_temp_table` for page accumulators.
pub fn accumulator_empty_select(plan: &VectorAggregatePlan) -> String {
    let mut columns = plan
        .group_keys
        .iter()
        .map(|column| {
            format!(
                "NULL::{} AS {}",
                vector_type_sql(column.value_type),
                quote_ident(&column.name)
            )
        })
        .collect::<Vec<_>>();
    columns.push("NULL::bigint AS __ins_count".to_string());
    columns.push("NULL::bigint AS __del_count".to_string());
    for aggregate in &plan.aggregates {
        let alias = &aggregate.output_alias;
        match aggregate.function {
            VectorAggregateFunction::CountStar | VectorAggregateFunction::Count => {
                columns.push(format!(
                    "NULL::bigint AS {}",
                    quote_ident(&format!("__ins_{alias}"))
                ));
                columns.push(format!(
                    "NULL::bigint AS {}",
                    quote_ident(&format!("__del_{alias}"))
                ));
            }
            VectorAggregateFunction::Sum => {
                for prefix in ["__ins_", "__del_", "__ins_nonnull_", "__del_nonnull_"] {
                    columns.push(format!(
                        "NULL::bigint AS {}",
                        quote_ident(&format!("{prefix}{alias}"))
                    ));
                }
            }
            VectorAggregateFunction::Avg => {
                for prefix in ["__ins_", "__del_", "__ins_count_", "__del_count_"] {
                    columns.push(format!(
                        "NULL::bigint AS {}",
                        quote_ident(&format!("{prefix}{alias}"))
                    ));
                }
            }
            VectorAggregateFunction::Min | VectorAggregateFunction::Max => {
                let value_type = aggregate
                    .input
                    .as_ref()
                    .map(|column| vector_type_sql(column.value_type))
                    .unwrap_or("bigint");
                columns.push(format!(
                    "NULL::{value_type} AS {}",
                    quote_ident(&format!("__ins_{alias}"))
                ));
                columns.push(format!(
                    "NULL::{value_type} AS {}",
                    quote_ident(&format!("__del_{alias}"))
                ));
            }
        }
    }
    format!("SELECT {} WHERE FALSE", columns.join(", "))
}

/// Build the narrow COUNT/SUM/AVG accumulator-to-logical-delta projection.
pub fn algebraic_finalize_sql(
    plan: &VectorAggregatePlan,
    target_relation: &str,
    accumulator_relation: &str,
    delta_relation: &str,
    insert_only_minmax: bool,
) -> Result<String, PgTrickleError> {
    let has_minmax = plan.aggregates.iter().any(|aggregate| {
        matches!(
            aggregate.function,
            VectorAggregateFunction::Min | VectorAggregateFunction::Max
        )
    });
    if plan.output_projection.is_some() {
        return Err(PgTrickleError::InvalidArgument(
            "vector finalizer requires an unprojected plan".to_string(),
        ));
    }
    let target = quote_qualified(target_relation)?;
    let accumulator = quote_qualified(accumulator_relation)?;
    let delta = quote_qualified(delta_relation)?;
    let consolidated = accumulator_consolidation_sql(plan, &accumulator);
    let needs_minmax_rescan = has_minmax && !insert_only_minmax;
    let (rescan_cte, rescan_join) = if needs_minmax_rescan {
        minmax_rescan_sql(plan)
    } else {
        (String::new(), String::new())
    };
    let consolidated_group_refs = plan
        .group_keys
        .iter()
        .map(|column| format!("d0.{}", quote_ident(&column.name)))
        .collect::<Vec<_>>();
    let group_row_id = if consolidated_group_refs.is_empty() {
        "pgtrickle.encode_row_id_v2('GROUP_KEY', ROW('__singleton_group'::text))".to_string()
    } else {
        crate::dvm::operators::scan::build_hash_expr_for_domain(
            "GROUP_KEY",
            &consolidated_group_refs,
        )
    };
    let group_refs = plan
        .group_keys
        .iter()
        .map(|column| format!("d.{}", quote_ident(&column.name)))
        .collect::<Vec<_>>();
    let row_id = "d.__pgt_group_row_id";
    let join = if plan.group_keys.is_empty() {
        "TRUE".to_string()
    } else {
        format!(
            "pgtrickle.row_probe_v1(st.__pgt_row_id) = pgtrickle.row_probe_v1({row_id}) AND st.__pgt_row_id = {row_id}"
        )
    };
    let new_count =
        "COALESCE(st.__pgt_count, 0) + COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)";
    let action = if plan.group_keys.is_empty() {
        "CASE WHEN st.__pgt_count IS NULL AND (COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)) > 0 THEN 'I' ELSE 'U' END"
    } else {
        "CASE WHEN st.__pgt_count IS NULL AND (COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)) > 0 THEN 'I' WHEN COALESCE(st.__pgt_count, 0) + COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0) <= 0 THEN 'D' ELSE 'U' END"
    };
    let mut merged = vec![format!(
        "COALESCE(st.__pgt_row_id, {row_id}) AS __pgt_row_id"
    )];
    merged.extend(group_refs.iter().cloned());
    merged.push(format!("{new_count} AS new_count"));
    merged.push("COALESCE(st.__pgt_count, 0) AS old_count".to_string());
    let mut final_values = Vec::new();
    let mut changes = vec!["m.new_count IS DISTINCT FROM m.old_count".to_string()];
    for aggregate in &plan.aggregates {
        let alias = &aggregate.output_alias;
        let quoted_alias = quote_ident(alias);
        let ins = quote_ident(&format!("__ins_{alias}"));
        let del = quote_ident(&format!("__del_{alias}"));
        let (new_value, aux_values) = match aggregate.function {
            VectorAggregateFunction::CountStar | VectorAggregateFunction::Count => (
                format!(
                    "COALESCE(st.{quoted_alias}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0)"
                ),
                Vec::new(),
            ),
            VectorAggregateFunction::Sum => {
                let aux = quote_ident(&format!("__pgt_aux_nonnull_{alias}"));
                let ins_nonnull = quote_ident(&format!("__ins_nonnull_{alias}"));
                let del_nonnull = quote_ident(&format!("__del_nonnull_{alias}"));
                let new_aux = format!(
                    "COALESCE(st.{aux}, 0) + COALESCE(d.{ins_nonnull}, 0) - COALESCE(d.{del_nonnull}, 0)"
                );
                (
                    format!(
                        "CASE WHEN ({new_aux}) > 0 THEN COALESCE(st.{quoted_alias}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0) ELSE NULL END"
                    ),
                    vec![(aux, new_aux)],
                )
            }
            VectorAggregateFunction::Avg => {
                let aux_sum = quote_ident(&format!("__pgt_aux_sum_{alias}"));
                let aux_count = quote_ident(&format!("__pgt_aux_count_{alias}"));
                let ins_count = quote_ident(&format!("__ins_count_{alias}"));
                let del_count = quote_ident(&format!("__del_count_{alias}"));
                let new_sum = format!(
                    "COALESCE(st.{aux_sum}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0)"
                );
                let new_aux_count = format!(
                    "COALESCE(st.{aux_count}, 0) + COALESCE(d.{ins_count}, 0) - COALESCE(d.{del_count}, 0)"
                );
                (
                    format!("({new_sum})::numeric / NULLIF(({new_aux_count}), 0)"),
                    vec![(aux_sum, new_sum), (aux_count, new_aux_count)],
                )
            }
            VectorAggregateFunction::Min => {
                let value = if needs_minmax_rescan {
                    format!("r.{quoted_alias}")
                } else {
                    format!("LEAST(st.{quoted_alias}, d.{ins})")
                };
                (value, Vec::new())
            }
            VectorAggregateFunction::Max => {
                let value = if needs_minmax_rescan {
                    format!("r.{quoted_alias}")
                } else {
                    format!("GREATEST(st.{quoted_alias}, d.{ins})")
                };
                (value, Vec::new())
            }
        };
        merged.push(format!(
            "{new_value} AS {}",
            quote_ident(&format!("new_{alias}"))
        ));
        merged.push(format!(
            "st.{quoted_alias} AS {}",
            quote_ident(&format!("old_{alias}"))
        ));
        changes.push(format!(
            "m.{}::text IS DISTINCT FROM m.{}::text",
            quote_ident(&format!("new_{alias}")),
            quote_ident(&format!("old_{alias}"))
        ));
        final_values.push(format!(
            "CASE WHEN m.__pgt_meta_action = 'D' THEN m.{} ELSE m.{} END AS {quoted_alias}",
            quote_ident(&format!("old_{alias}")),
            quote_ident(&format!("new_{alias}"))
        ));
        for (aux, value) in aux_values {
            let merged_alias = quote_ident(&format!("new_{}", aux.trim_matches('"')));
            let old_alias = quote_ident(&format!("old_{}", aux.trim_matches('"')));
            merged.push(format!("{value} AS {merged_alias}"));
            merged.push(format!("st.{aux} AS {old_alias}"));
            changes.push(format!("m.{merged_alias} IS DISTINCT FROM m.{old_alias}"));
            final_values.push(format!(
                "CASE WHEN m.__pgt_meta_action = 'D' THEN 0 ELSE m.{merged_alias} END AS {aux}"
            ));
        }
    }
    merged.push(format!("{action} AS __pgt_meta_action"));
    let final_groups = plan
        .group_keys
        .iter()
        .map(|column| format!("m.{}", quote_ident(&column.name)))
        .collect::<Vec<_>>();
    let mut outputs = vec![
        "m.__pgt_row_id".to_string(),
        "CASE WHEN m.__pgt_meta_action = 'D' THEN 'D' ELSE 'I' END AS __pgt_action".to_string(),
    ];
    outputs.extend(final_groups);
    outputs.push(
        "CASE WHEN m.__pgt_meta_action = 'D' THEN m.old_count ELSE m.new_count END AS __pgt_count"
            .to_string(),
    );
    outputs.extend(final_values);
    Ok(format!(
        "INSERT INTO {delta} WITH d0 AS ({consolidated}), d AS (SELECT d0.*, {group_row_id} AS __pgt_group_row_id FROM d0){rescan_cte}, m AS (SELECT {} FROM d LEFT JOIN {target} st ON {join}{rescan_join}) SELECT {} FROM m WHERE m.__pgt_meta_action IN ('I', 'D') OR (m.__pgt_meta_action = 'U' AND ({}))",
        merged.join(", "),
        outputs.join(", "),
        changes.join(" OR ")
    ))
}

fn minmax_rescan_sql(plan: &VectorAggregatePlan) -> (String, String) {
    let source = format!(
        "{}.{}",
        quote_ident(&plan.source_schema),
        quote_ident(&plan.source_table)
    );
    let source_groups = plan
        .group_keys
        .iter()
        .map(|column| format!("src.{}", quote_ident(&column.name)))
        .collect::<Vec<_>>();
    let aggregates = plan
        .aggregates
        .iter()
        .filter_map(|aggregate| {
            let function = match aggregate.function {
                VectorAggregateFunction::Min => "MIN",
                VectorAggregateFunction::Max => "MAX",
                _ => return None,
            };
            let input = quote_ident(&aggregate.input.as_ref()?.name);
            Some(format!(
                "{function}(src.{input}) AS {}",
                quote_ident(&aggregate.output_alias)
            ))
        })
        .collect::<Vec<_>>();
    if source_groups.is_empty() {
        return (
            format!(
                ", r AS (SELECT {} FROM {source} src)",
                aggregates.join(", ")
            ),
            " LEFT JOIN r ON TRUE".to_string(),
        );
    }
    let source_row_id = crate::dvm::operators::scan::build_hash_expr_for_domain(
        "GROUP_KEY",
        &plan
            .group_keys
            .iter()
            .map(|column| format!("src.{}", quote_ident(&column.name)))
            .collect::<Vec<_>>(),
    );
    let source_to_delta = format!(
        "pgtrickle.row_probe_v1({source_row_id}) = pgtrickle.row_probe_v1(d.__pgt_group_row_id) AND {source_row_id} = d.__pgt_group_row_id"
    );
    let rescan_to_delta = "pgtrickle.row_probe_v1(r.__pgt_group_row_id) = pgtrickle.row_probe_v1(d.__pgt_group_row_id) AND r.__pgt_group_row_id = d.__pgt_group_row_id";
    let mut selects = source_groups.clone();
    selects.push(format!("{source_row_id} AS __pgt_group_row_id"));
    selects.extend(aggregates);
    (
        format!(
            ", r AS (SELECT {} FROM {source} src JOIN d ON {source_to_delta} GROUP BY {})",
            selects.join(", "),
            source_groups.join(", ")
        ),
        format!(" LEFT JOIN r ON {rescan_to_delta}"),
    )
}

fn accumulator_consolidation_sql(plan: &VectorAggregatePlan, accumulator: &str) -> String {
    let groups = plan
        .group_keys
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect::<Vec<_>>();
    let mut columns = groups.clone();
    columns.extend(
        ["__ins_count", "__del_count"]
            .into_iter()
            .map(|name| format!("SUM({name}) AS {name}")),
    );
    for aggregate in &plan.aggregates {
        let alias = &aggregate.output_alias;
        let transition_columns: &[&str] = match aggregate.function {
            VectorAggregateFunction::CountStar | VectorAggregateFunction::Count => {
                &["__ins_", "__del_"]
            }
            VectorAggregateFunction::Sum => {
                &["__ins_", "__del_", "__ins_nonnull_", "__del_nonnull_"]
            }
            VectorAggregateFunction::Avg => &["__ins_", "__del_", "__ins_count_", "__del_count_"],
            VectorAggregateFunction::Min | VectorAggregateFunction::Max => &["__ins_", "__del_"],
        };
        let combine = match aggregate.function {
            VectorAggregateFunction::Min => "MIN",
            VectorAggregateFunction::Max => "MAX",
            _ => "SUM",
        };
        columns.extend(transition_columns.iter().map(|prefix| {
            let name = quote_ident(&format!("{prefix}{alias}"));
            format!("{combine}({name}) AS {name}")
        }));
    }
    if groups.is_empty() {
        format!(
            "SELECT {} FROM {accumulator} HAVING COUNT(*) > 0",
            columns.join(", ")
        )
    } else {
        format!(
            "SELECT {} FROM {accumulator} GROUP BY {}",
            columns.join(", "),
            groups.join(", ")
        )
    }
}

fn vector_type_sql(value_type: VectorType) -> &'static str {
    match value_type {
        VectorType::Bool => "boolean",
        VectorType::Int2 => "smallint",
        VectorType::Int4 => "integer",
        VectorType::Int8 => "bigint",
        VectorType::Date => "date",
        VectorType::Timestamp => "timestamp without time zone",
        VectorType::TimestampTz => "timestamp with time zone",
    }
}

#[cfg(test)]
pub fn execute_vectorized_aggregate(
    _plan: &VectorAggregatePlan,
    _execution: &VectorAggregateExecution<'_>,
) -> Result<VectorAggregateStats, PgTrickleError> {
    Err(PgTrickleError::InternalError(
        "vector execution requires a PostgreSQL backend".to_string(),
    ))
}

fn checked_stat_add(left: u64, right: u64) -> Result<u64, PgTrickleError> {
    left.checked_add(right)
        .ok_or_else(|| PgTrickleError::InternalError("vector metric overflow".to_string()))
}

#[cfg(not(test))]
fn page_row_limit(plan: &VectorAggregatePlan, byte_limit: u64) -> std::os::raw::c_long {
    let row_width = 1usize
        + plan
            .group_keys
            .iter()
            .chain(
                plan.aggregates
                    .iter()
                    .filter_map(|aggregate| aggregate.input.as_ref()),
            )
            .map(|column| match column.value_type {
                VectorType::Bool => 2,
                VectorType::Int2 => 3,
                VectorType::Int4 | VectorType::Date => 5,
                VectorType::Int8 | VectorType::Timestamp | VectorType::TimestampTz => 9,
            })
            .sum::<usize>();
    ((byte_limit as usize / row_width.max(1)).clamp(1, VECTOR_PAGE_ROWS)) as std::os::raw::c_long
}

#[cfg(not(test))]
fn source_query(plan: &VectorAggregatePlan) -> Result<String, PgTrickleError> {
    let relation = quote_qualified(plan.change_buffer.as_str())?;
    let mut columns = vec!["action::text AS action".to_string()];
    columns.extend(
        plan.group_keys
            .iter()
            .map(|column| quote_ident(&column.name)),
    );
    columns.extend(plan.aggregates.iter().filter_map(|aggregate| {
        aggregate
            .input
            .as_ref()
            .map(|column| quote_ident(&column.name))
    }));
    Ok(format!(
        "SELECT {} FROM {relation} WHERE lsn > $1::pg_lsn AND lsn <= $2::pg_lsn AND action IN ('I', 'D')",
        columns.join(", ")
    ))
}

#[cfg(not(test))]
fn fetch_page(
    cursor_name: &str,
    plan: &VectorAggregatePlan,
    fetch_rows: std::os::raw::c_long,
) -> Result<Option<VectorPage>, PgTrickleError> {
    Spi::connect(|client| {
        let mut cursor = client
            .find_cursor(cursor_name)
            .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        let table = cursor
            .fetch(fetch_rows)
            .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        if table.is_empty() {
            drop(cursor);
            return Ok::<Option<VectorPage>, PgTrickleError>(None);
        }
        let page = copy_page(table, plan)?;
        cursor.detach_into_name();
        Ok(Some(page))
    })
}

#[cfg(not(test))]
fn copy_page(
    table: pgrx::spi::SpiTupleTable<'_>,
    plan: &VectorAggregatePlan,
) -> Result<VectorPage, PgTrickleError> {
    let capacity = table.len();
    let mut page = VectorPage {
        actions: Vec::with_capacity(capacity),
        group_columns: plan
            .group_keys
            .iter()
            .map(|column| OwnedColumn::with_capacity(column.value_type, capacity))
            .collect(),
        aggregate_columns: plan
            .aggregates
            .iter()
            .map(|aggregate| {
                aggregate
                    .input
                    .as_ref()
                    .map(|column| OwnedColumn::with_capacity(column.value_type, capacity))
            })
            .collect(),
    };
    for tuple in table {
        let action = tuple
            .get::<String>(1)
            .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("NULL vector action".to_string()))?;
        page.actions.push(match action.as_str() {
            "I" => 1,
            "D" => -1,
            _ => {
                return Err(PgTrickleError::InternalError(format!(
                    "invalid vector action '{action}'"
                )));
            }
        });
        let mut ordinal = 2;
        for (column, column_plan) in page.group_columns.iter_mut().zip(&plan.group_keys) {
            column.push_datum(read_datum(&tuple, ordinal, column_plan.value_type)?);
            ordinal += 1;
        }
        for (column, aggregate) in page.aggregate_columns.iter_mut().zip(&plan.aggregates) {
            if let (Some(column), Some(input)) = (column, aggregate.input.as_ref()) {
                column.push_datum(read_datum(&tuple, ordinal, input.value_type)?);
                ordinal += 1;
            }
        }
    }
    Ok(page)
}

#[cfg(not(test))]
fn read_datum(
    tuple: &pgrx::spi::SpiHeapTupleData<'_>,
    ordinal: usize,
    value_type: VectorType,
) -> Result<Option<pg_sys::Datum>, PgTrickleError> {
    let entry = tuple
        .get_datum_by_ordinal(ordinal)
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    if entry.oid().to_u32() != value_type.oid() {
        return Err(PgTrickleError::TypeMismatch(format!(
            "vector column {ordinal} changed from OID {} to OID {}",
            value_type.oid(),
            entry.oid().to_u32()
        )));
    }
    entry
        .value::<pgrx::AnyElement>()
        .map(|value| value.map(|value| value.datum()))
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))
}

#[cfg(not(test))]
fn insert_groups(
    plan: &VectorAggregatePlan,
    relation_name: &str,
    groups: &[ReducedGroup],
) -> Result<(), PgTrickleError> {
    let relation_oid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT to_regclass($1)::oid",
        &[relation_name.into()],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InternalError(format!(
            "vector accumulator relation '{relation_name}' is missing"
        ))
    })?;
    let expected_oids = accumulator_oids(plan);
    let relation = unsafe {
        // SAFETY: relation_oid resolved in this backend and transaction.
        pg_sys::table_open(relation_oid, pg_sys::RowExclusiveLock as _)
    };
    if relation.is_null() {
        return Err(PgTrickleError::InternalError(
            "vector accumulator relation could not be opened".to_string(),
        ));
    }
    let result = (|| {
        let descriptor = unsafe {
            // SAFETY: table_open returned a live relation.
            (*relation).rd_att
        };
        let columns = unsafe {
            // SAFETY: rd_att is the live relation descriptor.
            (*descriptor).natts as usize
        };
        if columns != expected_oids.len() {
            return Err(PgTrickleError::TypeMismatch(
                "vector accumulator column count changed".to_string(),
            ));
        }
        for (index, expected_oid) in expected_oids.iter().enumerate() {
            let actual_oid = unsafe {
                // SAFETY: index is within the validated descriptor length.
                (*pg_sys::TupleDescAttr(descriptor, index as _))
                    .atttypid
                    .to_u32()
            };
            if actual_oid != *expected_oid {
                return Err(PgTrickleError::TypeMismatch(format!(
                    "vector accumulator column {} changed from OID {expected_oid} to OID {actual_oid}",
                    index + 1
                )));
            }
        }
        let slot = unsafe {
            // SAFETY: descriptor belongs to the open accumulator relation.
            pg_sys::MakeSingleTupleTableSlot(descriptor, &pg_sys::TTSOpsHeapTuple as *const _)
        };
        if slot.is_null() {
            return Err(PgTrickleError::InternalError(
                "vector accumulator slot allocation failed".to_string(),
            ));
        }
        let insert_result = (|| {
            for group in groups {
                let (values, nulls) = group_datums(group);
                let tuple = unsafe {
                    // SAFETY: values and nulls match the validated descriptor.
                    pg_sys::heap_form_tuple(descriptor, values.as_ptr(), nulls.as_ptr())
                };
                if tuple.is_null() {
                    return Err(PgTrickleError::InternalError(
                        "vector accumulator tuple formation failed".to_string(),
                    ));
                }
                unsafe {
                    // SAFETY: tuple matches the open relation descriptor and
                    // slot; clearing frees the owned tuple before slot reuse.
                    pg_sys::ExecStoreHeapTuple(tuple, slot, true);
                    pg_sys::table_tuple_insert(
                        relation,
                        slot,
                        pg_sys::GetCurrentCommandId(true),
                        0,
                        std::ptr::null_mut(),
                    );
                    pg_sys::ExecClearTuple(slot);
                }
            }
            Ok(())
        })();
        unsafe {
            // SAFETY: slot was allocated above and is empty after each insert.
            pg_sys::ExecDropSingleTupleTableSlot(slot);
        }
        insert_result?;
        unsafe {
            // SAFETY: inserts above completed in the current command.
            pg_sys::CommandCounterIncrement();
        }
        Ok(())
    })();
    unsafe {
        // SAFETY: relation was opened above with this lock mode.
        pg_sys::table_close(relation, pg_sys::RowExclusiveLock as _);
    }
    result
}

#[cfg(not(test))]
fn accumulator_oids(plan: &VectorAggregatePlan) -> Vec<u32> {
    let mut oids = plan
        .group_keys
        .iter()
        .map(|column| column.value_type.oid())
        .collect::<Vec<_>>();
    oids.extend([pg_sys::INT8OID.to_u32(), pg_sys::INT8OID.to_u32()]);
    for aggregate in &plan.aggregates {
        match aggregate.function {
            VectorAggregateFunction::CountStar | VectorAggregateFunction::Count => {
                oids.extend([pg_sys::INT8OID.to_u32(), pg_sys::INT8OID.to_u32()]);
            }
            VectorAggregateFunction::Sum | VectorAggregateFunction::Avg => {
                oids.extend([
                    pg_sys::INT8OID.to_u32(),
                    pg_sys::INT8OID.to_u32(),
                    pg_sys::INT8OID.to_u32(),
                    pg_sys::INT8OID.to_u32(),
                ]);
            }
            VectorAggregateFunction::Min | VectorAggregateFunction::Max => {
                oids.extend([aggregate.result_type_oid, aggregate.result_type_oid]);
            }
        }
    }
    oids
}

#[cfg(not(test))]
fn group_datums(group: &ReducedGroup) -> (Vec<pg_sys::Datum>, Vec<bool>) {
    let mut values = Vec::new();
    let mut nulls = Vec::new();
    for value in &group.key {
        push_scalar(&mut values, &mut nulls, *value);
    }
    push_i64(&mut values, &mut nulls, group.inserted_rows);
    push_i64(&mut values, &mut nulls, group.deleted_rows);
    for transition in &group.transitions {
        match transition {
            AggregateTransition::Count { inserted, deleted } => {
                push_i64(&mut values, &mut nulls, *inserted);
                push_i64(&mut values, &mut nulls, *deleted);
            }
            AggregateTransition::Sum {
                inserted,
                deleted,
                inserted_nonnull,
                deleted_nonnull,
            } => {
                for value in [inserted, deleted, inserted_nonnull, deleted_nonnull] {
                    push_i64(&mut values, &mut nulls, *value);
                }
            }
            AggregateTransition::Avg {
                inserted_sum,
                deleted_sum,
                inserted_count,
                deleted_count,
            } => {
                for value in [inserted_sum, deleted_sum, inserted_count, deleted_count] {
                    push_i64(&mut values, &mut nulls, *value);
                }
            }
            AggregateTransition::MinMax { inserted, deleted } => {
                push_scalar(&mut values, &mut nulls, *inserted);
                push_scalar(&mut values, &mut nulls, *deleted);
            }
        }
    }
    (values, nulls)
}

#[cfg(not(test))]
fn push_i64(values: &mut Vec<pg_sys::Datum>, nulls: &mut Vec<bool>, value: i64) {
    values.push(pg_sys::Datum::from(value));
    nulls.push(false);
}

#[cfg(not(test))]
fn push_scalar(values: &mut Vec<pg_sys::Datum>, nulls: &mut Vec<bool>, value: Option<ScalarValue>) {
    nulls.push(value.is_none());
    values.push(match value {
        Some(ScalarValue::Bool(value)) => pg_sys::Datum::from(value),
        Some(ScalarValue::Int2(value)) => pg_sys::Datum::from(value),
        Some(ScalarValue::Int4(value) | ScalarValue::Date(value)) => pg_sys::Datum::from(value),
        Some(
            ScalarValue::Int8(value)
            | ScalarValue::Timestamp(value)
            | ScalarValue::TimestampTz(value),
        ) => pg_sys::Datum::from(value),
        None => pg_sys::Datum::from(0usize),
    });
}

#[cfg(not(test))]
fn truncate_relation(relation: &str) -> Result<(), PgTrickleError> {
    let relation = quote_qualified(relation)?;
    Spi::run(&format!("TRUNCATE TABLE {relation}")) // nosemgrep: rust.spi.run.dynamic-format -- relation is identifier-quoted.
        .map_err(|error| PgTrickleError::SpiError(format!("vector batch cleanup: {error}")))
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_qualified(value: &str) -> Result<String, PgTrickleError> {
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(PgTrickleError::InvalidArgument(format!(
            "invalid vector relation name '{value}'"
        )));
    }
    Ok(parts
        .into_iter()
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join("."))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::vectorized_agg::{VectorAggregateExprPlan, VectorColumnPlan};

    fn plan() -> VectorAggregatePlan {
        VectorAggregatePlan {
            source_oid: 42,
            source_schema: "public".into(),
            source_table: "source".into(),
            change_buffer: "pgtrickle_changes.changes_42".into(),
            group_keys: vec![VectorColumnPlan {
                name: "account_id".into(),
                value_type: VectorType::Int4,
                nullable: false,
            }],
            aggregates: vec![
                VectorAggregateExprPlan {
                    function: VectorAggregateFunction::CountStar,
                    input: None,
                    result_type_oid: 20,
                    output_alias: "rows".into(),
                    auxiliary_columns: Vec::new(),
                },
                VectorAggregateExprPlan {
                    function: VectorAggregateFunction::Sum,
                    input: Some(VectorColumnPlan {
                        name: "amount".into(),
                        value_type: VectorType::Int4,
                        nullable: true,
                    }),
                    result_type_oid: 20,
                    output_alias: "total".into(),
                    auxiliary_columns: vec!["__pgt_aux_nonnull_total".into()],
                },
            ],
            target_output_order: vec!["account_id".into(), "rows".into(), "total".into()],
            frontier_placeholders: ("p".into(), "n".into()),
            output_projection: None,
        }
    }

    #[test]
    fn reduces_insert_delete_and_null_semantics_per_group() {
        let page = VectorPage {
            actions: vec![1, 1, -1, 1],
            group_columns: vec![OwnedColumn::Int4(vec![7, 7, 7, 9], vec![false; 4])],
            aggregate_columns: vec![
                None,
                Some(OwnedColumn::Int4(
                    vec![10, 0, 3, 5],
                    vec![false, true, false, false],
                )),
            ],
        };
        let groups = reduce_page(&plan(), &page).expect("valid page");
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].inserted_rows, 2);
        assert_eq!(groups[0].deleted_rows, 1);
        assert_eq!(
            groups[0].transitions[0],
            AggregateTransition::Count {
                inserted: 2,
                deleted: 1
            }
        );
        assert_eq!(
            groups[0].transitions[1],
            AggregateTransition::Sum {
                inserted: 10,
                deleted: 3,
                inserted_nonnull: 1,
                deleted_nonnull: 1
            }
        );
    }

    #[test]
    fn rejects_invalid_action_before_reduction() {
        let page = VectorPage {
            actions: vec![0],
            group_columns: vec![OwnedColumn::Int4(vec![7], vec![false])],
            aggregate_columns: vec![None, Some(OwnedColumn::Int4(vec![1], vec![false]))],
        };
        assert!(matches!(
            reduce_page(&plan(), &page),
            Err(PgTrickleError::InternalError(_))
        ));
    }

    #[test]
    fn combines_same_group_across_pages() {
        let first = VectorPage {
            actions: vec![1],
            group_columns: vec![OwnedColumn::Int4(vec![7], vec![false])],
            aggregate_columns: vec![None, Some(OwnedColumn::Int4(vec![10], vec![false]))],
        };
        let second = VectorPage {
            actions: vec![-1, 1],
            group_columns: vec![OwnedColumn::Int4(vec![7, 7], vec![false; 2])],
            aggregate_columns: vec![None, Some(OwnedColumn::Int4(vec![3, 5], vec![false; 2]))],
        };
        let plan = plan();
        let mut target = reduce_page(&plan, &first).expect("first page").remove(0);
        let incoming = reduce_page(&plan, &second).expect("second page").remove(0);
        combine_group(&plan, &mut target, &incoming).expect("compatible groups");
        assert_eq!((target.inserted_rows, target.deleted_rows), (2, 1));
        assert_eq!(
            target.transitions[1],
            AggregateTransition::Sum {
                inserted: 15,
                deleted: 3,
                inserted_nonnull: 2,
                deleted_nonnull: 1,
            }
        );
        assert!(aggregate_group_limit(&plan, 64 * 1024 * 1024) >= 10_000);
    }

    #[test]
    fn min_max_tracks_page_candidates() {
        let aggregate = VectorAggregateExprPlan {
            function: VectorAggregateFunction::Min,
            input: Some(VectorColumnPlan {
                name: "value".into(),
                value_type: VectorType::Int8,
                nullable: true,
            }),
            result_type_oid: 20,
            output_alias: "low".into(),
            auxiliary_columns: Vec::new(),
        };
        let mut plan = plan();
        plan.aggregates = vec![aggregate];
        let page = VectorPage {
            actions: vec![1, 1, -1],
            group_columns: vec![OwnedColumn::Int4(vec![1, 1, 1], vec![false; 3])],
            aggregate_columns: vec![Some(OwnedColumn::Int8(vec![9, 3, 2], vec![false; 3]))],
        };
        let groups = reduce_page(&plan, &page).expect("valid min page");
        assert_eq!(
            groups[0].transitions[0],
            AggregateTransition::MinMax {
                inserted: Some(ScalarValue::Int8(3)),
                deleted: Some(ScalarValue::Int8(2)),
            }
        );
    }

    #[test]
    fn avg_finalizer_uses_numeric_division() {
        let mut plan = plan();
        plan.aggregates = vec![VectorAggregateExprPlan {
            function: VectorAggregateFunction::Avg,
            input: Some(VectorColumnPlan {
                name: "amount".into(),
                value_type: VectorType::Int4,
                nullable: true,
            }),
            result_type_oid: 1700,
            output_alias: "mean".into(),
            auxiliary_columns: vec!["__pgt_aux_sum_mean".into(), "__pgt_aux_count_mean".into()],
        }];
        let sql = algebraic_finalize_sql(
            &plan,
            "public.target",
            "pg_temp.acc",
            "pg_temp.delta",
            false,
        )
        .expect("supported finalizer");
        assert!(sql.contains("::numeric / NULLIF"));
    }

    #[test]
    fn finalizer_consolidates_page_groups_before_apply() {
        let sql = algebraic_finalize_sql(
            &plan(),
            "public.target",
            "pg_temp.acc",
            "pg_temp.delta",
            false,
        )
        .expect("supported finalizer");
        assert!(sql.contains("WITH d0 AS (SELECT \"account_id\", SUM(__ins_count) AS __ins_count"));
        assert!(sql.contains("GROUP BY \"account_id\"), d AS"));
        assert!(sql.contains(
            "pgtrickle.row_probe_v1(st.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_group_row_id) AND st.__pgt_row_id = d.__pgt_group_row_id"
        ));

        let mut singleton = plan();
        singleton.group_keys.clear();
        let sql = algebraic_finalize_sql(
            &singleton,
            "public.target",
            "pg_temp.acc",
            "pg_temp.delta",
            false,
        )
        .expect("supported singleton finalizer");
        assert!(sql.contains("HAVING COUNT(*) > 0), d AS"));
    }

    #[test]
    fn minmax_deletion_uses_one_batched_source_rescan() {
        let mut minmax = plan();
        minmax.aggregates = vec![VectorAggregateExprPlan {
            function: VectorAggregateFunction::Min,
            input: Some(VectorColumnPlan {
                name: "amount".into(),
                value_type: VectorType::Int4,
                nullable: true,
            }),
            result_type_oid: 23,
            output_alias: "low".into(),
            auxiliary_columns: Vec::new(),
        }];
        let sql = algebraic_finalize_sql(
            &minmax,
            "public.target",
            "pg_temp.acc",
            "pg_temp.delta",
            false,
        )
        .expect("supported MIN repair");
        assert!(sql.contains("AS __pgt_group_row_id, MIN(src.\"amount\") AS \"low\""));
        assert!(sql.contains("FROM \"public\".\"source\" src JOIN d ON"));
        assert!(sql.contains("r.__pgt_group_row_id = d.__pgt_group_row_id"));
    }

    #[test]
    fn transition_overflow_is_reported() {
        assert!(matches!(
            checked_add(i64::MAX, 1, "total"),
            Err(PgTrickleError::VectorAggregateOverflow { aggregate }) if aggregate == "total"
        ));
    }
}
