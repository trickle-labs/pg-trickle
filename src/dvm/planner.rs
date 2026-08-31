//! Evidence and bounded shadow planning for delta operator scheduling.

use crate::dvm::parser::{Expr, OpTree, ParseResult};
use crate::error::PgTrickleError;
use pgrx::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub const FORMAT_VERSION: u16 = 1;
const PLANNING_BUDGET: Duration = Duration::from_millis(10);
const MINIMUM_IMPROVEMENT: f64 = 0.05;
const MAX_EPOCH_SOURCES: usize = 256;

#[derive(Clone, Debug, Serialize)]
pub struct ColumnStatistics {
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub collation_oid: u32,
    pub null_fraction: Option<f64>,
    pub distinct_count: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RelationStatistics {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub estimated_rows: Option<f64>,
    pub pages: Option<i64>,
    pub last_analyze_epoch: Option<f64>,
    pub last_autoanalyze_epoch: Option<f64>,
    pub columns: Vec<ColumnStatistics>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PlanObservation {
    /// Actual rows in the final delta relation from the latest completed
    /// differential refresh. This is not a per-operator row count.
    pub actual_intermediate_rows: Option<i64>,
    pub last_delta_rows: Option<i64>,
    pub last_runtime_us: Option<i64>,
    pub source: Option<&'static str>,
}

#[derive(Clone, Debug)]
pub struct PlanningEvidence {
    pub source_oids: Vec<u32>,
    pub relations: Vec<RelationStatistics>,
    pub observation: PlanObservation,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Estimate {
    pub rows: Option<f64>,
    pub formula: &'static str,
    pub evidence: &'static str,
}

impl Estimate {
    fn known(rows: f64, formula: &'static str, evidence: &'static str) -> Self {
        Self {
            rows: rows.is_finite().then_some(rows.max(0.0)),
            formula,
            evidence,
        }
    }

    fn unknown(formula: &'static str) -> Self {
        Self {
            rows: None,
            formula,
            evidence: "unknown",
        }
    }
}

#[derive(Serialize)]
struct PlanSummary {
    operator_order: Vec<String>,
    estimated_intermediate_rows: Option<f64>,
    observed_intermediate_rows: Option<i64>,
    observed_runtime_us: Option<i64>,
}

#[derive(Serialize)]
struct RuleDecision {
    rule: &'static str,
    operator_ids: Vec<String>,
    before: Estimate,
    after: Estimate,
    evidence_source: &'static str,
    validation_status: &'static str,
    selected: bool,
}

#[derive(Serialize)]
struct SkippedRule {
    rule: &'static str,
    code: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct VectorPathEvidence {
    pub eligible: bool,
    pub selected: bool,
    pub fallback_reason: Option<String>,
}

impl Default for VectorPathEvidence {
    fn default() -> Self {
        Self {
            eligible: false,
            selected: false,
            fallback_reason: Some("NOT_EVALUATED_BY_LT9".into()),
        }
    }
}

/// Map MT-8's pure admission result into the shared diagnostic contract.
pub fn vector_path_evidence(tree: &OpTree) -> VectorPathEvidence {
    use crate::dvm::operators::vectorized_agg::{
        VectorAggregateAdmission, VectorAggregateFallback, VectorizedAggregateOperator,
    };

    match VectorizedAggregateOperator::plan(
        tree,
        Some("pgtrickle_changes.__diagnostic__"),
        Some(("0/0", "0/0")),
        VectorAggregateAdmission::default(),
    ) {
        Ok(plan) => {
            let selected = plan.output_projection.is_none();
            VectorPathEvidence {
                eligible: true,
                selected,
                fallback_reason: if plan.output_projection.is_some() {
                    Some(
                        VectorAggregateFallback::UnsupportedProjection
                            .as_str()
                            .into(),
                    )
                } else {
                    None
                },
            }
        }
        Err(reason) => VectorPathEvidence {
            eligible: false,
            selected: false,
            fallback_reason: Some(reason.as_str().into()),
        },
    }
}

#[derive(Serialize)]
pub struct DeltaPlanDiagnostic {
    format_version: u16,
    pgt_id: i64,
    statistics_epoch: String,
    statistics_complete: bool,
    observations: PlanObservation,
    vector_path: VectorPathEvidence,
    original: PlanSummary,
    candidate: Option<PlanSummary>,
    chosen: PlanSummary,
    rules: Vec<RuleDecision>,
    skipped_rules: Vec<SkippedRule>,
    planning_time_us: u64,
    planning_timed_out: bool,
}

#[derive(Clone)]
struct ScanInput {
    oid: u32,
    alias: String,
    label: String,
    original_position: usize,
}

#[derive(Clone)]
struct JoinEdge {
    left: usize,
    left_column: String,
    right: usize,
    right_column: String,
}

#[derive(Clone)]
struct JoinState {
    order: Vec<usize>,
    rows: f64,
    cost: f64,
}

struct JoinSchedule {
    original: JoinState,
    candidate: JoinState,
    timed_out: bool,
}

/// Collect all catalog evidence in one SPI scope and return owned values.
pub fn collect_evidence(
    pgt_id: i64,
    parsed: &ParseResult,
) -> Result<PlanningEvidence, PgTrickleError> {
    let mut oids = parsed.tree.source_oids();
    oids.extend(parsed.cte_registry.source_oids());
    oids.sort_unstable();
    oids.dedup();

    Spi::connect(|client| {
        let mut relations = Vec::<RelationStatistics>::new();
        if !oids.is_empty() {
            let oid_list = oids
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT c.oid, n.nspname::text, c.relname::text, \
                        CASE WHEN c.reltuples >= 0 THEN c.reltuples::float8 END, \
                        c.relpages::bigint, EXTRACT(epoch FROM s.last_analyze)::float8, \
                        EXTRACT(epoch FROM s.last_autoanalyze)::float8, \
                        a.attname::text, a.atttypid, a.atttypmod, a.attcollation, \
                        ps.null_frac::float8, ps.n_distinct::float8 \
                   FROM pg_catalog.pg_class c \
                   JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                   LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid \
                   LEFT JOIN pg_catalog.pg_attribute a \
                     ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
                   LEFT JOIN pg_catalog.pg_stats ps \
                     ON ps.schemaname = n.nspname AND ps.tablename = c.relname \
                    AND ps.attname = a.attname AND NOT ps.inherited \
                  WHERE c.oid IN ({oid_list}) \
                  ORDER BY c.oid, a.attnum"
            );
            let rows = client
                .select(&sql, None, &[])
                .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;

            for row in rows {
                let oid = row
                    .get::<pgrx::pg_sys::Oid>(1)
                    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
                    .map(pgrx::pg_sys::Oid::to_u32)
                    .ok_or_else(|| {
                        PgTrickleError::InternalError("planner relation OID is null".into())
                    })?;
                let index = match relations.last().filter(|relation| relation.oid == oid) {
                    Some(_) => relations.len() - 1,
                    None => {
                        relations.push(RelationStatistics {
                            oid,
                            schema: row.get::<String>(2).ok().flatten().unwrap_or_default(),
                            name: row.get::<String>(3).ok().flatten().unwrap_or_default(),
                            estimated_rows: finite(row.get::<f64>(4).ok().flatten()),
                            pages: row.get::<i64>(5).ok().flatten(),
                            last_analyze_epoch: finite(row.get::<f64>(6).ok().flatten()),
                            last_autoanalyze_epoch: finite(row.get::<f64>(7).ok().flatten()),
                            columns: Vec::new(),
                        });
                        relations.len() - 1
                    }
                };
                if let Some(name) = row.get::<String>(8).ok().flatten() {
                    let raw_distinct = finite(row.get::<f64>(13).ok().flatten());
                    let relation_rows = relations[index].estimated_rows;
                    relations[index].columns.push(ColumnStatistics {
                        name,
                        type_oid: row
                            .get::<pgrx::pg_sys::Oid>(9)
                            .ok()
                            .flatten()
                            .map(pgrx::pg_sys::Oid::to_u32)
                            .unwrap_or(0),
                        typmod: row.get::<i32>(10).ok().flatten().unwrap_or(-1),
                        collation_oid: row
                            .get::<pgrx::pg_sys::Oid>(11)
                            .ok()
                            .flatten()
                            .map(pgrx::pg_sys::Oid::to_u32)
                            .unwrap_or(0),
                        null_fraction: finite(row.get::<f64>(12).ok().flatten())
                            .map(|value| value.clamp(0.0, 1.0)),
                        distinct_count: normalize_distinct(raw_distinct, relation_rows),
                    });
                }
            }
        }

        let observation_rows = client
            .select(
                "SELECT delta_row_count, \
                        (EXTRACT(epoch FROM (end_time - start_time)) * 1000000)::bigint \
                   FROM pgtrickle.pgt_refresh_history \
                  WHERE pgt_id = $1 AND status = 'COMPLETED' \
                    AND action = 'DIFFERENTIAL' AND end_time IS NOT NULL \
                  ORDER BY refresh_id DESC LIMIT 1",
                None,
                &[pgt_id.into()],
            )
            .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        let (last_delta_rows, last_runtime_us) = if observation_rows.is_empty() {
            (None, None)
        } else {
            let observation = observation_rows.first();
            (
                observation.get::<i64>(1).ok().flatten(),
                observation.get::<i64>(2).ok().flatten(),
            )
        };

        Ok(PlanningEvidence {
            source_oids: oids,
            relations,
            observation: PlanObservation {
                actual_intermediate_rows: last_delta_rows,
                last_delta_rows,
                last_runtime_us,
                source: (last_delta_rows.is_some() || last_runtime_us.is_some())
                    .then_some("pgt_refresh_history"),
            },
        })
    })
}

/// Build the stable, side-effect-free LT-9 diagnostic.
pub fn build_delta_plan(
    pgt_id: i64,
    tree: &OpTree,
    evidence: &PlanningEvidence,
    vector_path: VectorPathEvidence,
) -> DeltaPlanDiagnostic {
    let started = Instant::now();
    let epoch = statistics_epoch(&evidence.relations);
    let statistics_complete = evidence.source_oids.iter().all(|oid| {
        evidence
            .relations
            .iter()
            .any(|relation| relation.oid == *oid && relation.estimated_rows.is_some())
    });
    let mut skipped_rules = vec![
        SkippedRule {
            rule: "selective_filter_pushdown",
            code: "SHADOW_RULE_NOT_YET_VALIDATED",
        },
        SkippedRule {
            rule: "bijective_distinct_projection",
            code: "SHADOW_RULE_NOT_YET_VALIDATED",
        },
    ];
    let mut rules = Vec::new();
    let mut original_order = scan_labels(tree);
    let mut original_cost = None;
    let mut candidate_summary = None;
    let mut timed_out = false;

    if let Some((inputs, edges)) = extract_inner_join_region(tree) {
        match schedule_inner_joins(&inputs, &edges, &evidence.relations, PLANNING_BUDGET) {
            Some(schedule) => {
                timed_out = schedule.timed_out;
                original_order = labels_for_order(&inputs, &schedule.original.order);
                original_cost = Some(schedule.original.cost);
                let candidate_order = labels_for_order(&inputs, &schedule.candidate.order);
                if schedule.candidate.order != schedule.original.order
                    && schedule.candidate.cost
                        <= schedule.original.cost * (1.0 - MINIMUM_IMPROVEMENT)
                {
                    let operator_ids = schedule
                        .candidate
                        .order
                        .iter()
                        .map(|index| stable_operator_id(&inputs[*index]))
                        .collect();
                    rules.push(RuleDecision {
                        rule: "inner_join_order",
                        operator_ids,
                        before: Estimate::known(
                            schedule.original.cost,
                            "sum(intermediate_rows)",
                            "pg_stats",
                        ),
                        after: Estimate::known(
                            schedule.candidate.cost,
                            "sum(intermediate_rows)",
                            "pg_stats",
                        ),
                        evidence_source: "pg_class+pg_stats",
                        validation_status: "shadow",
                        selected: false,
                    });
                    candidate_summary = Some(PlanSummary {
                        operator_order: candidate_order,
                        estimated_intermediate_rows: Some(schedule.candidate.cost),
                        observed_intermediate_rows: None,
                        observed_runtime_us: None,
                    });
                } else {
                    skipped_rules.push(SkippedRule {
                        rule: "inner_join_order",
                        code: "NO_ESTIMATED_IMPROVEMENT",
                    });
                }
            }
            None => skipped_rules.push(SkippedRule {
                rule: "inner_join_order",
                code: "MISSING_OR_UNSUPPORTED_STATISTICS",
            }),
        }
    } else {
        skipped_rules.push(SkippedRule {
            rule: "inner_join_order",
            code: "NO_SAFE_INNER_EQUIJOIN_REGION",
        });
    }

    let original = PlanSummary {
        operator_order: original_order.clone(),
        estimated_intermediate_rows: original_cost,
        observed_intermediate_rows: evidence.observation.actual_intermediate_rows,
        observed_runtime_us: evidence.observation.last_runtime_us,
    };
    let chosen = PlanSummary {
        operator_order: original_order,
        estimated_intermediate_rows: original_cost,
        observed_intermediate_rows: evidence.observation.actual_intermediate_rows,
        observed_runtime_us: evidence.observation.last_runtime_us,
    };

    DeltaPlanDiagnostic {
        format_version: FORMAT_VERSION,
        pgt_id,
        statistics_epoch: epoch,
        statistics_complete,
        observations: evidence.observation.clone(),
        vector_path,
        original,
        candidate: candidate_summary,
        chosen,
        rules,
        skipped_rules,
        planning_time_us: started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        planning_timed_out: timed_out,
    }
}

/// Return a cheap relation-statistics epoch for cache validation.
///
/// The probe reads a bounded number of catalog rows in one SPI scope. Missing
/// relations, catalog errors, and oversized source sets fail closed to the
/// distinct `unknown` epoch.
pub fn statistics_epoch_for_sources(source_oids: &[u32]) -> String {
    let mut oids = source_oids.to_vec();
    oids.sort_unstable();
    oids.dedup();
    if oids.len() > MAX_EPOCH_SOURCES {
        return "unknown".into();
    }
    if oids.is_empty() {
        return hash_epoch(&format!("planner:{FORMAT_VERSION};empty"));
    }

    let oid_list = oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let result: Result<String, PgTrickleError> = Spi::connect(|client| {
        let sql = format!(
            "SELECT c.oid, c.relnamespace, c.relname::text, c.relkind::text, \
                    c.relfilenode, c.reltuples::float8, c.relpages::bigint, \
                    EXTRACT(epoch FROM s.last_analyze)::float8, \
                    EXTRACT(epoch FROM s.last_autoanalyze)::float8, \
                    s.analyze_count, s.autoanalyze_count, s.vacuum_count, s.autovacuum_count \
               FROM pg_catalog.pg_class c \
               LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid \
              WHERE c.oid IN ({oid_list}) ORDER BY c.oid"
        );
        let rows = client
            .select(&sql, None, &[])
            .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        if rows.len() != oids.len() {
            return Ok("unknown".into());
        }
        let mut canonical = format!("planner:{FORMAT_VERSION};");
        for row in rows {
            let oid = row
                .get::<pgrx::pg_sys::Oid>(1)
                .ok()
                .flatten()
                .map(pgrx::pg_sys::Oid::to_u32)
                .ok_or_else(|| PgTrickleError::InternalError("planner epoch OID is null".into()))?;
            let namespace = row
                .get::<pgrx::pg_sys::Oid>(2)
                .ok()
                .flatten()
                .map(pgrx::pg_sys::Oid::to_u32)
                .ok_or_else(|| {
                    PgTrickleError::InternalError("planner epoch namespace is null".into())
                })?;
            let values = (
                row.get::<String>(3).ok().flatten(),
                row.get::<String>(4).ok().flatten(),
                row.get::<pgrx::pg_sys::Oid>(5)
                    .ok()
                    .flatten()
                    .map(pgrx::pg_sys::Oid::to_u32),
                finite(row.get::<f64>(6).ok().flatten()),
                row.get::<i64>(7).ok().flatten(),
                finite(row.get::<f64>(8).ok().flatten()),
                finite(row.get::<f64>(9).ok().flatten()),
                row.get::<i64>(10).ok().flatten(),
                row.get::<i64>(11).ok().flatten(),
                row.get::<i64>(12).ok().flatten(),
                row.get::<i64>(13).ok().flatten(),
            );
            canonical.push_str(&format!("{oid}:{namespace}:{values:?};"));
        }
        Ok(hash_epoch(&canonical))
    });
    result.unwrap_or_else(|_| "unknown".into())
}

pub fn equality_selectivity(null_fraction: Option<f64>, distinct_count: Option<f64>) -> Estimate {
    match (finite(null_fraction), finite(distinct_count)) {
        (Some(nulls), Some(distinct)) if distinct > 0.0 => Estimate::known(
            ((1.0 - nulls.clamp(0.0, 1.0)) / distinct.max(1.0)).clamp(0.0, 1.0),
            "(1-null_fraction)/max(n_distinct,1)",
            "n_distinct",
        ),
        _ => Estimate::unknown("(1-null_fraction)/max(n_distinct,1)"),
    }
}

pub fn null_selectivity(null_fraction: Option<f64>, is_null: bool) -> Estimate {
    match finite(null_fraction) {
        Some(value) => Estimate::known(
            if is_null {
                value.clamp(0.0, 1.0)
            } else {
                1.0 - value.clamp(0.0, 1.0)
            },
            if is_null {
                "null_fraction"
            } else {
                "1-null_fraction"
            },
            "null_fraction",
        ),
        None => Estimate::unknown(if is_null {
            "null_fraction"
        } else {
            "1-null_fraction"
        }),
    }
}

pub fn and_selectivity(left: &Estimate, right: &Estimate) -> Estimate {
    match (left.rows, right.rows) {
        (Some(left), Some(right)) => {
            Estimate::known((left * right).clamp(0.0, 1.0), "left*right", "independence")
        }
        _ => Estimate::unknown("left*right"),
    }
}

pub fn or_selectivity(left: &Estimate, right: &Estimate) -> Estimate {
    match (left.rows, right.rows) {
        (Some(left), Some(right)) => Estimate::known(
            (left + right - left * right).clamp(0.0, 1.0),
            "left+right-left*right",
            "inclusion_exclusion",
        ),
        _ => Estimate::unknown("left+right-left*right"),
    }
}

fn finite(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite())
}

fn normalize_distinct(raw: Option<f64>, rows: Option<f64>) -> Option<f64> {
    match raw {
        Some(value) if value > 0.0 => Some(value),
        Some(value) if value < 0.0 => rows.map(|rows| (-value * rows).max(1.0)),
        _ => None,
    }
}

fn statistics_epoch(relations: &[RelationStatistics]) -> String {
    let mut canonical = format!("planner:{FORMAT_VERSION};");
    for relation in relations {
        canonical.push_str(&format!(
            "{}:{}:{}:{:?}:{:?}:{:?}:{:?};",
            relation.oid,
            relation.schema,
            relation.name,
            relation.estimated_rows,
            relation.pages,
            relation.last_analyze_epoch,
            relation.last_autoanalyze_epoch
        ));
        for column in &relation.columns {
            canonical.push_str(&format!(
                "{}:{}:{}:{}:{:?}:{:?};",
                column.name,
                column.type_oid,
                column.typmod,
                column.collation_oid,
                column.null_fraction,
                column.distinct_count
            ));
        }
    }
    hash_epoch(&canonical)
}

fn hash_epoch(canonical: &str) -> String {
    format!("{:016x}", xxhash_rust::xxh3::xxh3_64(canonical.as_bytes()))
}

fn scan_labels(tree: &OpTree) -> Vec<String> {
    let mut labels = Vec::new();
    collect_scan_labels(tree, &mut labels);
    labels
}

fn collect_scan_labels(tree: &OpTree, labels: &mut Vec<String>) {
    match tree {
        OpTree::Scan {
            schema, table_name, ..
        } => labels.push(format!("{schema}.{table_name}")),
        OpTree::Project { child, .. }
        | OpTree::Filter { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Distinct { child }
        | OpTree::Subquery { child, .. }
        | OpTree::Window { child, .. }
        | OpTree::LateralFunction { child, .. }
        | OpTree::LateralSubquery { child, .. }
        | OpTree::ScalarSubquery { child, .. } => collect_scan_labels(child, labels),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. }
        | OpTree::Intersect { left, right, .. }
        | OpTree::Except { left, right, .. } => {
            collect_scan_labels(left, labels);
            collect_scan_labels(right, labels);
        }
        OpTree::UnionAll { children } => {
            for child in children {
                collect_scan_labels(child, labels);
            }
        }
        OpTree::CteScan { body, .. } => {
            if let Some(body) = body {
                collect_scan_labels(body, labels);
            }
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            collect_scan_labels(base, labels);
            collect_scan_labels(recursive, labels);
        }
        OpTree::RecursiveSelfRef { .. } | OpTree::ConstantSelect { .. } => {}
    }
}

fn extract_inner_join_region(tree: &OpTree) -> Option<(Vec<ScanInput>, Vec<JoinEdge>)> {
    let mut tree = tree;
    while let OpTree::Project { child, .. }
    | OpTree::Aggregate { child, .. }
    | OpTree::Distinct { child } = tree
    {
        tree = child;
    }
    let OpTree::InnerJoin { .. } = tree else {
        return None;
    };
    let mut inputs = Vec::new();
    let mut equalities = Vec::new();
    flatten_inner_join(tree, &mut inputs, &mut equalities)?;
    if inputs.len() < 2 {
        return None;
    }
    let aliases = inputs
        .iter()
        .enumerate()
        .map(|(index, input)| (input.alias.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut edges = Vec::new();
    for (left_alias, left_column, right_alias, right_column) in equalities {
        let (&left, &right) = (
            aliases.get(left_alias.as_str())?,
            aliases.get(right_alias.as_str())?,
        );
        if left == right {
            return None;
        }
        edges.push(JoinEdge {
            left,
            left_column,
            right,
            right_column,
        });
    }
    Some((inputs, edges))
}

fn flatten_inner_join(
    tree: &OpTree,
    inputs: &mut Vec<ScanInput>,
    equalities: &mut Vec<(String, String, String, String)>,
) -> Option<()> {
    match tree {
        OpTree::Scan {
            table_oid,
            schema,
            table_name,
            alias,
            ..
        } => {
            inputs.push(ScanInput {
                oid: *table_oid,
                alias: alias.clone(),
                label: format!("{schema}.{table_name}"),
                original_position: inputs.len(),
            });
            Some(())
        }
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            flatten_inner_join(left, inputs, equalities)?;
            flatten_inner_join(right, inputs, equalities)?;
            collect_equality_conjuncts(condition, equalities)
        }
        _ => None,
    }
}

fn collect_equality_conjuncts(
    expression: &Expr,
    output: &mut Vec<(String, String, String, String)>,
) -> Option<()> {
    match expression {
        Expr::BinaryOp { op, left, right } if op.eq_ignore_ascii_case("and") => {
            collect_equality_conjuncts(left, output)?;
            collect_equality_conjuncts(right, output)
        }
        Expr::BinaryOp { op, left, right } if op == "=" => match (left.as_ref(), right.as_ref()) {
            (
                Expr::ColumnRef {
                    table_alias: Some(left_alias),
                    column_name: left_column,
                },
                Expr::ColumnRef {
                    table_alias: Some(right_alias),
                    column_name: right_column,
                },
            ) => {
                output.push((
                    left_alias.clone(),
                    left_column.clone(),
                    right_alias.clone(),
                    right_column.clone(),
                ));
                Some(())
            }
            _ => None,
        },
        _ => None,
    }
}

fn schedule_inner_joins(
    inputs: &[ScanInput],
    edges: &[JoinEdge],
    relations: &[RelationStatistics],
    budget: Duration,
) -> Option<JoinSchedule> {
    let started = Instant::now();
    if inputs.len() > 8 {
        return greedy_join_order(inputs, edges, relations, started, budget);
    }
    // ponytail: exhaustive subsets stop at eight inputs; use the greedy path if
    // measured planner time approaches the 10 ms budget.
    dynamic_join_order(inputs, edges, relations, started, budget)
}

fn dynamic_join_order(
    inputs: &[ScanInput],
    edges: &[JoinEdge],
    relations: &[RelationStatistics],
    started: Instant,
    budget: Duration,
) -> Option<JoinSchedule> {
    let original = cost_order(
        &(0..inputs.len()).collect::<Vec<_>>(),
        inputs,
        edges,
        relations,
    )?;
    let mut states = vec![None::<JoinState>; 1 << inputs.len()];
    for index in 0..inputs.len() {
        states[1 << index] = Some(JoinState {
            order: vec![index],
            rows: input_rows(&inputs[index], relations)?,
            cost: 0.0,
        });
    }
    let mut timed_out = false;
    for mask in 1..states.len() {
        if started.elapsed() >= budget {
            timed_out = true;
            break;
        }
        let Some(state) = states[mask].clone() else {
            continue;
        };
        for next in 0..inputs.len() {
            if mask & (1 << next) != 0 {
                continue;
            }
            let Some(rows) = joined_rows(mask, state.rows, next, inputs, edges, relations) else {
                continue;
            };
            let mut candidate = state.clone();
            candidate.order.push(next);
            candidate.rows = rows;
            candidate.cost += rows;
            let next_mask = mask | (1 << next);
            if better_state(&candidate, states[next_mask].as_ref(), inputs) {
                states[next_mask] = Some(candidate);
            }
        }
    }
    let candidate = states
        .last()
        .and_then(Clone::clone)
        .unwrap_or_else(|| original.clone());
    Some(JoinSchedule {
        original,
        candidate,
        timed_out,
    })
}

fn greedy_join_order(
    inputs: &[ScanInput],
    edges: &[JoinEdge],
    relations: &[RelationStatistics],
    started: Instant,
    budget: Duration,
) -> Option<JoinSchedule> {
    let original = cost_order(
        &(0..inputs.len()).collect::<Vec<_>>(),
        inputs,
        edges,
        relations,
    )?;
    let mut first = (0..inputs.len()).min_by(|left, right| {
        input_rows(&inputs[*left], relations)
            .partial_cmp(&input_rows(&inputs[*right], relations))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| input_key(&inputs[*left]).cmp(&input_key(&inputs[*right])))
    })?;
    let mut mask = 1 << first;
    let mut state = JoinState {
        order: vec![first],
        rows: input_rows(&inputs[first], relations)?,
        cost: 0.0,
    };
    let mut timed_out = false;
    while state.order.len() < inputs.len() {
        if started.elapsed() >= budget {
            timed_out = true;
            return Some(JoinSchedule {
                original: original.clone(),
                candidate: original,
                timed_out,
            });
        }
        let next = (0..inputs.len())
            .filter(|index| mask & (1 << index) == 0)
            .filter_map(|index| {
                joined_rows(mask, state.rows, index, inputs, edges, relations)
                    .map(|rows| (index, rows))
            })
            .min_by(|(left_index, left_rows), (right_index, right_rows)| {
                left_rows
                    .partial_cmp(right_rows)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        input_key(&inputs[*left_index]).cmp(&input_key(&inputs[*right_index]))
                    })
            })?;
        first = next.0;
        state.order.push(first);
        state.rows = next.1;
        state.cost += next.1;
        mask |= 1 << first;
    }
    Some(JoinSchedule {
        original,
        candidate: state,
        timed_out,
    })
}

fn cost_order(
    order: &[usize],
    inputs: &[ScanInput],
    edges: &[JoinEdge],
    relations: &[RelationStatistics],
) -> Option<JoinState> {
    let first = *order.first()?;
    let mut mask = 1 << first;
    let mut state = JoinState {
        order: vec![first],
        rows: input_rows(&inputs[first], relations)?,
        cost: 0.0,
    };
    for &next in &order[1..] {
        state.rows = joined_rows(mask, state.rows, next, inputs, edges, relations)?;
        state.cost += state.rows;
        state.order.push(next);
        mask |= 1 << next;
    }
    Some(state)
}

fn joined_rows(
    mask: usize,
    current_rows: f64,
    next: usize,
    inputs: &[ScanInput],
    edges: &[JoinEdge],
    relations: &[RelationStatistics],
) -> Option<f64> {
    edges
        .iter()
        .filter_map(|edge| {
            if edge.right == next && mask & (1 << edge.left) != 0 {
                join_rows(
                    current_rows,
                    column(&inputs[edge.left], &edge.left_column, relations)?,
                    input_rows(&inputs[next], relations)?,
                    column(&inputs[next], &edge.right_column, relations)?,
                )
            } else if edge.left == next && mask & (1 << edge.right) != 0 {
                join_rows(
                    current_rows,
                    column(&inputs[edge.right], &edge.right_column, relations)?,
                    input_rows(&inputs[next], relations)?,
                    column(&inputs[next], &edge.left_column, relations)?,
                )
            } else {
                None
            }
        })
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
}

fn join_rows(
    left_rows: f64,
    left: &ColumnStatistics,
    right_rows: f64,
    right: &ColumnStatistics,
) -> Option<f64> {
    let left_distinct = left.distinct_count?;
    let right_distinct = right.distinct_count?;
    let left_non_null = left_rows * (1.0 - left.null_fraction?.clamp(0.0, 1.0));
    let right_non_null = right_rows * (1.0 - right.null_fraction?.clamp(0.0, 1.0));
    finite(Some(
        (left_non_null * right_non_null / left_distinct.max(right_distinct).max(1.0))
            .min(left_rows * right_rows),
    ))
}

fn input_rows(input: &ScanInput, relations: &[RelationStatistics]) -> Option<f64> {
    relations
        .iter()
        .find(|relation| relation.oid == input.oid)?
        .estimated_rows
        .map(|rows| rows.max(1.0))
}

fn column<'a>(
    input: &ScanInput,
    name: &str,
    relations: &'a [RelationStatistics],
) -> Option<&'a ColumnStatistics> {
    relations
        .iter()
        .find(|relation| relation.oid == input.oid)?
        .columns
        .iter()
        .find(|column| column.name == name)
}

fn better_state(candidate: &JoinState, current: Option<&JoinState>, inputs: &[ScanInput]) -> bool {
    match current {
        None => true,
        Some(current) if candidate.cost < current.cost => true,
        Some(current) if candidate.cost == current.cost => {
            order_key(&candidate.order, inputs) < order_key(&current.order, inputs)
        }
        _ => false,
    }
}

fn input_key(input: &ScanInput) -> (u32, usize) {
    (input.oid, input.original_position)
}

fn order_key(order: &[usize], inputs: &[ScanInput]) -> Vec<(u32, usize)> {
    order
        .iter()
        .map(|index| input_key(&inputs[*index]))
        .collect()
}

fn labels_for_order(inputs: &[ScanInput], order: &[usize]) -> Vec<String> {
    order
        .iter()
        .map(|index| inputs[*index].label.clone())
        .collect()
}

fn stable_operator_id(input: &ScanInput) -> String {
    format!("scan:{}:{}", input.oid, input.alias)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column_stats(name: &str, distinct_count: f64) -> ColumnStatistics {
        ColumnStatistics {
            name: name.into(),
            type_oid: 23,
            typmod: -1,
            collation_oid: 0,
            null_fraction: Some(0.0),
            distinct_count: Some(distinct_count),
        }
    }

    fn relation(oid: u32, rows: f64, columns: Vec<ColumnStatistics>) -> RelationStatistics {
        RelationStatistics {
            oid,
            schema: "public".into(),
            name: format!("t{oid}"),
            estimated_rows: Some(rows),
            pages: Some(1),
            last_analyze_epoch: None,
            last_autoanalyze_epoch: None,
            columns,
        }
    }

    fn scan(oid: u32, alias: &str) -> OpTree {
        OpTree::Scan {
            table_oid: oid,
            table_name: format!("t{oid}"),
            schema: "public".into(),
            columns: vec![],
            pk_columns: vec![],
            alias: alias.into(),
        }
    }

    fn equality(
        left_alias: &str,
        left_column: &str,
        right_alias: &str,
        right_column: &str,
    ) -> Expr {
        Expr::BinaryOp {
            op: "=".into(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some(left_alias.into()),
                column_name: left_column.into(),
            }),
            right: Box::new(Expr::ColumnRef {
                table_alias: Some(right_alias.into()),
                column_name: right_column.into(),
            }),
        }
    }

    fn three_way_tree() -> OpTree {
        OpTree::InnerJoin {
            condition: equality("b", "c", "c", "c"),
            left: Box::new(OpTree::InnerJoin {
                condition: equality("a", "b", "b", "b"),
                left: Box::new(scan(1, "a")),
                right: Box::new(scan(2, "b")),
            }),
            right: Box::new(scan(3, "c")),
        }
    }

    #[test]
    fn selectivity_formulas_preserve_unknown_and_clamp() {
        assert_eq!(equality_selectivity(Some(0.2), Some(4.0)).rows, Some(0.2));
        assert_eq!(equality_selectivity(None, Some(4.0)).rows, None);
        assert_eq!(null_selectivity(Some(1.4), false).rows, Some(0.0));
        let left = Estimate::known(0.8, "test", "test");
        let right = Estimate::known(0.5, "test", "test");
        assert_eq!(and_selectivity(&left, &right).rows, Some(0.4));
        assert!((or_selectivity(&left, &right).rows.unwrap() - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn shadow_join_candidate_never_changes_chosen_order() {
        let tree = three_way_tree();
        let evidence = PlanningEvidence {
            source_oids: vec![1, 2, 3],
            relations: vec![
                relation(1, 10_000.0, vec![column_stats("b", 10.0)]),
                relation(
                    2,
                    10_000.0,
                    vec![column_stats("b", 10.0), column_stats("c", 10_000.0)],
                ),
                relation(3, 10.0, vec![column_stats("c", 10.0)]),
            ],
            observation: PlanObservation::default(),
        };
        let value = serde_json::to_value(build_delta_plan(
            42,
            &tree,
            &evidence,
            VectorPathEvidence::default(),
        ))
        .unwrap();

        assert_eq!(value["rules"][0]["validation_status"], "shadow");
        assert_eq!(value["rules"][0]["selected"], false);
        assert!(value["candidate"]["observed_runtime_us"].is_null());
        assert!(value["observations"]["actual_intermediate_rows"].is_null());
        assert_ne!(
            value["candidate"]["operator_order"],
            value["original"]["operator_order"]
        );
        assert_eq!(
            value["chosen"]["operator_order"],
            value["original"]["operator_order"]
        );
    }

    #[test]
    fn missing_join_statistics_keep_the_current_plan() {
        let tree = three_way_tree();
        let evidence = PlanningEvidence {
            source_oids: vec![1, 2, 3],
            relations: vec![
                relation(1, 10.0, vec![]),
                relation(2, 10.0, vec![]),
                relation(3, 10.0, vec![]),
            ],
            observation: PlanObservation::default(),
        };
        let value = serde_json::to_value(build_delta_plan(
            7,
            &tree,
            &evidence,
            VectorPathEvidence::default(),
        ))
        .unwrap();

        assert!(value["candidate"].is_null());
        assert_eq!(
            value["chosen"]["operator_order"],
            value["original"]["operator_order"]
        );
    }

    #[test]
    fn zero_budget_falls_back_to_the_original_order() {
        let tree = three_way_tree();
        let (inputs, edges) = extract_inner_join_region(&tree).unwrap();
        let relations = vec![
            relation(1, 10.0, vec![column_stats("b", 10.0)]),
            relation(
                2,
                10.0,
                vec![column_stats("b", 10.0), column_stats("c", 10.0)],
            ),
            relation(3, 10.0, vec![column_stats("c", 10.0)]),
        ];
        let schedule = schedule_inner_joins(&inputs, &edges, &relations, Duration::ZERO).unwrap();

        assert!(schedule.timed_out);
        assert_eq!(schedule.candidate.order, schedule.original.order);
    }

    #[test]
    fn statistics_epoch_is_stable_and_sensitive_to_used_values() {
        let mut relations = vec![relation(1, 10.0, vec![column_stats("id", 10.0)])];
        let first = statistics_epoch(&relations);
        assert_eq!(first, statistics_epoch(&relations));
        relations[0].estimated_rows = Some(11.0);
        assert_ne!(first, statistics_epoch(&relations));
    }

    #[test]
    fn vector_path_evidence_is_passed_through_without_planner_coupling() {
        let tree = scan(1, "a");
        let evidence = PlanningEvidence {
            source_oids: vec![1],
            relations: vec![relation(1, 10.0, vec![])],
            observation: PlanObservation::default(),
        };
        let value = serde_json::to_value(build_delta_plan(
            1,
            &tree,
            &evidence,
            VectorPathEvidence {
                eligible: true,
                selected: true,
                fallback_reason: None,
            },
        ))
        .unwrap();

        assert_eq!(value["vector_path"]["eligible"], true);
        assert_eq!(value["vector_path"]["selected"], true);
        assert!(value["vector_path"]["fallback_reason"].is_null());
    }

    #[test]
    fn bounded_epoch_probe_rejects_oversized_source_sets_without_spi() {
        let source_oids = (1..=MAX_EPOCH_SOURCES as u32 + 1).collect::<Vec<_>>();
        assert_eq!(statistics_epoch_for_sources(&source_oids), "unknown");
        assert_eq!(
            statistics_epoch_for_sources(&[]),
            statistics_epoch_for_sources(&[])
        );
    }
}
