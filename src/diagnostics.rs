//! Phase 2 (DT-1 – DT-4): SQL-callable diagnostic / introspection functions.
//!
//! All four functions are `#[pg_extern(schema = "pgtrickle")]` with no
//! catalog side-effects — they are pure read-only introspection helpers.
//!
//! ## Functions
//! - [`explain_query_rewrite`] (DT-1): walk a query through every DVM rewrite
//!   pass and report which passes fired, plus detected DVM patterns.
//! - [`diagnose_errors`] (DT-2): return the last 5 FAILED refresh events for
//!   a stream table, classified by error type with remediation hints.
//! - [`list_auxiliary_columns`] (DT-3): list all `__pgt_*` internal columns
//!   on a stream table's storage relation and explain their purpose.
//! - [`validate_query`] (DT-4): parse and validate a query through the DVM
//!   pipeline without creating a stream table; return detected constructs,
//!   warnings, and the resolved refresh mode.

use pgrx::prelude::*;
use serde::Serialize;

use crate::catalog::StreamTableMeta;
use crate::dvm;
use crate::dvm::parser::{AggExpr, OpTree};
use crate::error::PgTrickleError;

/// The bounded, evidence-aware diagnostic snapshot shared by the text and
/// JSON explainers.
#[derive(Debug, Clone)]
pub(crate) struct StreamTableExplanation {
    pub(crate) refresh_mode: String,
    pub(crate) estimated_changed_rows: Option<i64>,
    pub(crate) expected_refresh_ms: Option<f64>,
    pub(crate) expected_refresh_source: &'static str,
    pub(crate) expected_refresh_samples: i32,
    pub(crate) current_lag_ms: Option<f64>,
    pub(crate) next_scheduled_refresh: String,
    pub(crate) full_fallback_threshold: Option<f64>,
    pub(crate) last_full_reason: Option<String>,
    pub(crate) last_full_reason_detail: Option<String>,
    pub(crate) dominant_cost: String,
    pub(crate) dominant_cost_units: Option<f64>,
    pub(crate) dominant_cost_source: &'static str,
    pub(crate) window: WindowDiagnostics,
}

/// One bounded registry row from `pgt_window_states`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct WindowStateDiagnostic {
    pub(crate) node_ordinal: i32,
    pub(crate) spec_ordinal: i32,
    pub(crate) status: String,
    pub(crate) schema_version: i16,
    pub(crate) strategy_version: i16,
    pub(crate) state_generation: i64,
    pub(crate) estimated_bytes: i64,
}

/// Read-only window strategy, state, and latest execution evidence.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct WindowDiagnostics {
    pub(crate) strategy: Option<serde_json::Value>,
    pub(crate) states: Vec<WindowStateDiagnostic>,
    pub(crate) estimated_bytes: u64,
    pub(crate) budget_bytes: u64,
    pub(crate) utilization_percent: f64,
    pub(crate) last_actual_strategy: Option<String>,
    pub(crate) last_fallback_reason: Option<String>,
    pub(crate) last_fallback_detail: Option<String>,
    pub(crate) estimated_emitted_rows: Option<u64>,
    pub(crate) crossover_evidence: Option<serde_json::Value>,
    pub(crate) reinitialization_required: bool,
}

fn strategy_requires_state(strategy: Option<&serde_json::Value>) -> bool {
    strategy
        .and_then(|strategy| strategy.get("nodes"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|node| node.get("functions")?.as_array())
        .flatten()
        .any(|function| {
            function
                .get("runtime_enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        })
}

fn window_runtime_evidence(
    action: Option<&str>,
    reason: Option<&str>,
    detail: Option<&str>,
    runtime_enabled: bool,
) -> (Option<String>, Option<u64>, Option<serde_json::Value>) {
    let detail = detail.and_then(|detail| serde_json::from_str::<serde_json::Value>(detail).ok());
    let strategy = detail
        .as_ref()
        .and_then(|detail| detail.get("strategy"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| match action {
            Some("FULL") => Some("full".into()),
            Some("REINITIALIZE") => Some("reinitialize".into()),
            Some("NO_DATA") => Some("no_data".into()),
            Some("IMMEDIATE") => Some("immediate_recompute".into()),
            Some("DIFFERENTIAL")
                if reason
                    .and_then(crate::refresh::RefreshReasonCode::from_str)
                    .and_then(crate::refresh::RefreshReasonCode::window_priority)
                    .is_some() =>
            {
                Some("partition_recompute".into())
            }
            Some("DIFFERENTIAL") if runtime_enabled => Some("incremental".into()),
            _ => None,
        });
    let emitted_rows = detail
        .as_ref()
        .and_then(|detail| detail.get("estimated_emitted_rows"))
        .and_then(serde_json::Value::as_u64);
    let crossover = detail.and_then(|mut detail| {
        detail
            .get_mut("crossover_evidence")
            .map(serde_json::Value::take)
    });
    (strategy, emitted_rows, crossover)
}

fn summarize_window_strategy(strategy: Option<&serde_json::Value>) -> String {
    let summaries = strategy
        .and_then(|strategy| strategy.get("nodes"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|node| node.get("functions")?.as_array())
        .flatten()
        .map(|function| {
            let kind = function
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            let selected = function
                .get("strategy")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            match function
                .get("fallback_reason")
                .and_then(serde_json::Value::as_str)
            {
                Some(reason) => format!("{kind}={selected} ({reason})"),
                None => format!("{kind}={selected}"),
            }
        })
        .collect::<Vec<_>>();
    if summaries.is_empty() {
        "none".into()
    } else {
        summaries.join(", ")
    }
}

fn gather_window_diagnostics(
    pgt_id: i64,
    needs_reinit: bool,
    refresh_mode: crate::dag::RefreshMode,
    strategy: Option<&crate::dvm::parser::WindowStrategyPlan>,
) -> Result<WindowDiagnostics, PgTrickleError> {
    let immediate_reason = if refresh_mode == crate::dag::RefreshMode::Immediate {
        strategy
            .map(crate::refresh::RefreshReason::from_immediate_window_plan)
            .transpose()?
            .flatten()
    } else {
        None
    };
    let strategy = strategy
        .map(crate::dvm::parser::WindowStrategyPlan::to_json)
        .transpose()
        .map_err(|reason| PgTrickleError::WindowStateInvalid {
            pgt_id,
            node_ordinal: -1,
            spec_ordinal: -1,
            reason,
        })?;
    let states = crate::window_state::entries_for_stream(pgt_id)?
        .into_iter()
        .map(|entry| WindowStateDiagnostic {
            node_ordinal: entry.node_ordinal,
            spec_ordinal: entry.spec_ordinal,
            status: entry.status.as_str().into(),
            schema_version: entry.schema_version,
            strategy_version: entry.strategy_version,
            state_generation: entry.state_generation,
            estimated_bytes: entry.estimated_bytes.max(0),
        })
        .collect::<Vec<_>>();
    let (latest_action, latest_reason, latest_detail, last_fallback_reason, last_fallback_detail) =
        Spi::connect(|client| {
            let latest = client.select(
                "SELECT action, refresh_reason, refresh_reason_detail \
                   FROM pgtrickle.pgt_refresh_history \
                  WHERE pgt_id = $1 AND status = 'COMPLETED' \
                  ORDER BY refresh_id DESC LIMIT 1",
                None,
                &[pgt_id.into()],
            )?;
            let latest = if latest.is_empty() {
                (None, None, None)
            } else {
                let row = latest.first();
                (
                    row.get::<String>(1)?,
                    row.get::<String>(2)?,
                    row.get::<String>(3)?,
                )
            };
            let fallback = client.select(
                "SELECT refresh_reason, refresh_reason_detail \
                   FROM pgtrickle.pgt_refresh_history \
                  WHERE pgt_id = $1 AND status = 'COMPLETED' \
                    AND refresh_reason LIKE 'WINDOW\\_%' ESCAPE '\\' \
                  ORDER BY refresh_id DESC LIMIT 1",
                None,
                &[pgt_id.into()],
            )?;
            let fallback = if fallback.is_empty() {
                (None, None)
            } else {
                let row = fallback.first();
                (row.get::<String>(1)?, row.get::<String>(2)?)
            };
            Ok::<_, pgrx::spi::SpiError>((latest.0, latest.1, latest.2, fallback.0, fallback.1))
        })
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    let estimated_bytes = states.iter().fold(0_u64, |total, state| {
        total.saturating_add(state.estimated_bytes as u64)
    });
    let budget_bytes = crate::config::pg_trickle_memory_budget().window_state_bytes;
    let utilization_percent = estimated_bytes as f64 * 100.0 / budget_bytes.max(1) as f64;
    let reinitialization_required = needs_reinit
        || states.iter().any(|state| state.status != "READY")
        || (strategy_requires_state(strategy.as_ref()) && states.is_empty());
    let runtime_enabled = strategy_requires_state(strategy.as_ref());
    let evidence_action = immediate_reason
        .as_ref()
        .map_or(latest_action.as_deref(), |_| Some("IMMEDIATE"));
    let evidence_reason = immediate_reason
        .as_ref()
        .map(|reason| reason.code.as_str())
        .or(latest_reason.as_deref());
    let evidence_detail = immediate_reason
        .as_ref()
        .map(|reason| reason.detail.as_str())
        .or(latest_detail.as_deref());
    let (last_actual_strategy, estimated_emitted_rows, crossover_evidence) =
        window_runtime_evidence(
            evidence_action,
            evidence_reason,
            evidence_detail,
            runtime_enabled,
        );
    let last_fallback_reason = immediate_reason
        .as_ref()
        .map(|reason| reason.code.as_str().to_string())
        .or(last_fallback_reason);
    let last_fallback_detail = immediate_reason
        .map(|reason| reason.detail)
        .or(last_fallback_detail);

    Ok(WindowDiagnostics {
        strategy,
        states,
        estimated_bytes,
        budget_bytes,
        utilization_percent,
        last_actual_strategy,
        last_fallback_reason,
        last_fallback_detail,
        estimated_emitted_rows,
        crossover_evidence,
        reinitialization_required,
    })
}

fn dominant_plan_cost(query: &str) -> Option<(String, f64)> {
    fn walk(node: &serde_json::Value) -> Option<(String, f64)> {
        let total = node.get("Total Cost")?.as_f64()?;
        let children = node
            .get("Plans")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let child_total = children
            .iter()
            .filter_map(|child| child.get("Total Cost").and_then(serde_json::Value::as_f64))
            .sum::<f64>();
        let exclusive = (total - child_total).max(0.0);
        let label = node
            .get("Relation Name")
            .and_then(serde_json::Value::as_str)
            .map(|relation| {
                format!(
                    "{} ({relation})",
                    node.get("Node Type")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown operator")
                )
            })
            .or_else(|| {
                node.get("Node Type")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "unknown operator".to_string());
        let mut best = (label, exclusive);
        for child in children {
            if let Some(candidate) = walk(child)
                && candidate.1 > best.1
            {
                best = candidate;
            }
        }
        Some(best)
    }

    let explain_sql = format!("EXPLAIN (FORMAT JSON, ANALYZE false) {query}");
    let plan_text = Spi::get_one::<String>(&explain_sql).unwrap_or(None)?;
    let explain: serde_json::Value = serde_json::from_str(&plan_text).ok()?;
    let root = explain.as_array()?.first()?.get("Plan")?;
    walk(root)
}

pub(crate) fn build_stream_table_explanation(
    name: &str,
) -> Result<StreamTableExplanation, PgTrickleError> {
    let (schema, table) = crate::api::helpers::parse_qualified_name_pub(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table)?;
    let requested_mode = Spi::get_one_with_args::<String>(
        "SELECT COALESCE(requested_refresh_mode, refresh_mode) FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
        &[st.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or_else(|| st.refresh_mode.as_str().to_string());
    let target_mode = Spi::get_one_with_args::<String>(
        "SELECT target_freshness_mode FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
        &[st.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    let summary = Spi::connect(|client| {
        let table = client
            .select(
                "SELECT s.last_full_reason, s.last_full_reason_detail,
                        (EXTRACT(EPOCH FROM (now() - COALESCE(st.last_refresh_at, st.created_at))) * 1000)::float8,
                        c.avg_full_ms::float8, c.avg_diff_ms::float8, c.sample_count
                   FROM pgtrickle.pgt_stream_tables st
              LEFT JOIN pgtrickle.pgt_refresh_summary s ON s.pgt_id = st.pgt_id
              LEFT JOIN pgtrickle.pgt_cost_model_summary c ON c.pgt_id = st.pgt_id
                  WHERE st.pgt_id = $1",
                None,
                &[st.pgt_id.into()],
            )?;
        let row = table.first();
        Ok::<_, pgrx::spi::SpiError>((
            row.get::<String>(1)?,
            row.get::<String>(2)?,
            row.get::<f64>(3)?,
            row.get::<f64>(4)?,
            row.get::<f64>(5)?,
            row.get::<i32>(6)?,
        ))
    })
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    let (
        last_full_reason,
        last_full_reason_detail,
        current_lag_ms,
        avg_full_ms,
        avg_diff_ms,
        samples,
    ) = summary;
    let effective = st.refresh_mode.as_str();
    let pending = crate::cdc::estimate_pending_changes(st.pgt_id);
    let expected_refresh_ms = if effective == "FULL" {
        avg_full_ms
    } else {
        avg_diff_ms.and_then(|per_delta_row| pending.map(|rows| per_delta_row * rows.max(0) as f64))
    };
    let dominant =
        crate::refresh::with_stream_owner(&st, || Ok(dominant_plan_cost(&st.defining_query)))?;
    let (dominant_cost, dominant_cost_units, dominant_cost_source) = dominant
        .map(|(label, cost)| (label, Some(cost), "postgres_explain"))
        .unwrap_or_else(|| {
            (
                st.query_complexity_class
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                None,
                "optree_classifier",
            )
        });
    let next_scheduled_refresh = match target_mode.as_deref() {
        Some("MANUAL") => "manual".to_string(),
        Some("ON_COMMIT") => "on_commit".to_string(),
        _ if st.schedule.is_some() => st.schedule.clone().unwrap_or_default(),
        _ => "unknown".to_string(),
    };
    let window_strategy =
        if st.window_strategy.is_some() || st.refresh_mode != crate::dag::RefreshMode::Full {
            crate::window_state::ensure_plan(&st)?
        } else {
            None
        };
    let window = gather_window_diagnostics(
        st.pgt_id,
        st.needs_reinit,
        st.refresh_mode,
        window_strategy.as_ref(),
    )?;
    Ok(StreamTableExplanation {
        refresh_mode: if requested_mode == effective {
            effective.to_string()
        } else {
            format!("{requested_mode} → {effective}")
        },
        estimated_changed_rows: pending,
        expected_refresh_ms,
        expected_refresh_source: if samples.unwrap_or(0) > 0 {
            if effective == "FULL" {
                "observed_full_history"
            } else {
                "observed_differential_history"
            }
        } else {
            "unavailable"
        },
        expected_refresh_samples: samples.unwrap_or(0),
        current_lag_ms,
        next_scheduled_refresh,
        full_fallback_threshold: requested_mode.eq_ignore_ascii_case("AUTO").then(|| {
            st.auto_threshold
                .unwrap_or_else(crate::config::pg_trickle_differential_max_change_ratio)
        }),
        last_full_reason,
        last_full_reason_detail,
        dominant_cost,
        dominant_cost_units,
        dominant_cost_source,
        window,
    })
}

pub(crate) fn render_stream_table_explanation_text(explanation: &StreamTableExplanation) -> String {
    let changed = explanation
        .estimated_changed_rows
        .map_or_else(|| "unknown".into(), |rows| rows.to_string());
    let expected = explanation
        .expected_refresh_ms
        .map_or_else(|| "unknown".into(), |ms| format!("~{ms:.1} ms"));
    let lag = explanation
        .current_lag_ms
        .map_or_else(|| "unknown".into(), |ms| format!("{ms:.0} ms"));
    let threshold = explanation
        .full_fallback_threshold
        .map_or_else(|| "not applicable".into(), |value| format!("{value:.3}"));
    let window_strategy = summarize_window_strategy(explanation.window.strategy.as_ref());
    let window_state = if explanation.window.states.is_empty() {
        "none".to_string()
    } else {
        explanation
            .window
            .states
            .iter()
            .map(|state| {
                format!(
                    "node {}/spec {}: {} generation {}",
                    state.node_ordinal, state.spec_ordinal, state.status, state.state_generation
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "Refresh mode: {}\nEstimated changed rows: {}\nDominant cost: {}\nExpected refresh time: {}\nCurrent lag: {}\nNext scheduled refresh: {}\nFULL fallback threshold/reason: {} / {}\nWindow strategy: {}\nWindow state: {}; {} / {} bytes ({:.1}%); reinitialization required={}\nLast window execution/fallback: {} / {}\nWrite-path overhead: unknown",
        explanation.refresh_mode,
        changed,
        explanation.dominant_cost,
        expected,
        lag,
        explanation.next_scheduled_refresh,
        threshold,
        explanation.last_full_reason.as_deref().unwrap_or("none"),
        window_strategy,
        window_state,
        explanation.window.estimated_bytes,
        explanation.window.budget_bytes,
        explanation.window.utilization_percent,
        explanation.window.reinitialization_required,
        explanation
            .window
            .last_actual_strategy
            .as_deref()
            .unwrap_or("none"),
        explanation
            .window
            .last_fallback_reason
            .as_deref()
            .unwrap_or("none"),
    )
}

pub(crate) fn render_stream_table_explanation_json(
    explanation: &StreamTableExplanation,
) -> serde_json::Value {
    serde_json::json!({
        "refresh_mode": explanation.refresh_mode,
        "estimated_changed_rows": explanation.estimated_changed_rows.map(|rows| serde_json::json!({"rows": rows, "source": "cdc_pending_changes"})),
        "dominant_cost": {"label": explanation.dominant_cost, "planner_cost_units": explanation.dominant_cost_units, "source": explanation.dominant_cost_source},
        "expected_refresh_time": {"milliseconds": explanation.expected_refresh_ms, "source": explanation.expected_refresh_source, "sample_count": explanation.expected_refresh_samples},
        "current_lag": {"milliseconds": explanation.current_lag_ms, "meaning": "time_since_last_successful_verification"},
        "next_scheduled_refresh": explanation.next_scheduled_refresh,
        "full_fallback": {"threshold": explanation.full_fallback_threshold, "reason": explanation.last_full_reason, "detail": explanation.last_full_reason_detail},
        "window": explanation.window,
        "write_path_overhead": null
    })
}

// ── DT-1: explain_query_rewrite ───────────────────────────────────────────

/// DT-1 — Walk a query through the full DVM rewrite pipeline and report
/// each pass.
///
/// Returns one row per rewrite pass. When a pass changes the query,
/// `changed = true` and `sql_after` contains the SQL text after the
/// transformation. When a pass is a no-op, `changed = false` and
/// `sql_after` is `NULL`.
///
/// Two synthetic rows are appended after all rewrite passes:
/// - `pass_name = 'topk_detection'` — `changed = true` when the query
///   matches the `ORDER BY … LIMIT n` TopK pattern; `sql_after` describes
///   the detected limit and ORDER BY clause.
/// - `pass_name = 'dvm_patterns'` — `changed = false`; `sql_after` is a
///   semicolon-separated list of DVM constructs detected in the parse tree
///   (e.g. `group_rescan_aggregates`, `full_join`, `recursive_cte`).
///
/// # SQL usage
/// ```sql
/// SELECT * FROM pgtrickle.explain_query_rewrite(
///   'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id'
/// );
/// ```
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle")]
pub fn explain_query_rewrite(
    query: &str,
) -> TableIterator<
    'static,
    (
        name!(pass_name, String),
        name!(changed, bool),
        name!(sql_after, Option<String>),
    ),
> {
    let rows = match explain_query_rewrite_impl(query) {
        Ok(r) => r,
        Err(e) => pgrx::error!("explain_query_rewrite: {}", e),
    };
    TableIterator::new(rows)
}

fn explain_query_rewrite_impl(
    query: &str,
) -> Result<Vec<(String, bool, Option<String>)>, PgTrickleError> {
    let mut rows: Vec<(String, bool, Option<String>)> = Vec::new();

    // Macro: apply one rewrite pass, record whether it changed the query.
    macro_rules! apply_pass {
        ($name:expr, $current:expr, $fn:expr) => {{
            let before = $current.clone();
            let after = $fn(&before)?;
            let changed = after != before;
            rows.push((
                $name.to_string(),
                changed,
                if changed { Some(after.clone()) } else { None },
            ));
            after
        }};
    }

    let q = apply_pass!(
        "view_inlining",
        query.to_string(),
        dvm::rewrite_views_inline
    );
    let q = apply_pass!("nested_window_lift", q, dvm::rewrite_nested_window_exprs);
    let q = apply_pass!("distinct_on", q, dvm::rewrite_distinct_on);
    let q = apply_pass!("grouping_sets", q, dvm::rewrite_grouping_sets);
    let q = apply_pass!(
        "scalar_subquery_in_where",
        q,
        dvm::rewrite_scalar_subquery_in_where
    );
    let q = apply_pass!(
        "correlated_scalar_in_select",
        q,
        dvm::rewrite_correlated_scalar_in_select
    );

    // SubLinks-in-OR + De Morgan normalization: up to 3 passes.
    let mut q = q;
    let mut sublinks_changed = false;
    for _ in 0..3 {
        let prev = q.clone();
        let q2 = dvm::rewrite_demorgan_sublinks(&q)?;
        let q2 = dvm::rewrite_sublinks_in_or(&q2)?;
        if q2 == prev {
            break;
        }
        q = q2;
        sublinks_changed = true;
    }
    rows.push((
        "sublinks_in_or_demorgan".to_string(),
        sublinks_changed,
        None,
    ));

    let q = apply_pass!("rows_from", q, dvm::rewrite_rows_from);

    // TopK detection.
    let topk = dvm::detect_topk_pattern(&q)?;
    let (effective_q, has_topk) = if let Some(ref info) = topk {
        rows.push((
            "topk_detection".to_string(),
            true,
            Some(format!(
                "TopK detected: LIMIT {}, ORDER BY: {}",
                info.limit_value, info.order_by_sql
            )),
        ));
        (info.base_query.clone(), true)
    } else {
        rows.push(("topk_detection".to_string(), false, None));
        (q.clone(), false)
    };

    // DVM parse — detect constructs present in the operator tree.
    if !has_topk {
        match dvm::parse_defining_query_full(&effective_q) {
            Ok(result) => {
                let mut patterns: Vec<String> = Vec::new();

                // IVM support verdict.
                match dvm::check_ivm_support_with_registry(&result) {
                    Ok(_) => patterns.push("ivm_support:DIFFERENTIAL".to_string()),
                    Err(e) => patterns.push(format!("ivm_support:FULL_ONLY({})", e)),
                }

                // Collect tree-level constructs.
                let constructs = collect_tree_constructs(&result.tree, &result.cte_registry);
                patterns.extend(constructs);

                // Recursion.
                if result.has_recursion {
                    patterns.push("recursive_cte:true".to_string());
                }

                // Auxiliary column needs.
                if dvm::query_needs_pgt_count(&effective_q) {
                    patterns.push("needs_pgt_count:true".to_string());
                }
                if dvm::query_needs_dual_count(&effective_q) {
                    patterns.push("needs_dual_count:true".to_string());
                }
                if dvm::query_needs_union_dedup_count(&effective_q) {
                    patterns.push("needs_union_dedup_count:true".to_string());
                }

                // Parse warnings.
                for w in &result.warnings {
                    patterns.push(format!("parse_warning:{}", w));
                }

                // Volatility: 'i' = immutable, 's' = stable, 'v' = volatile.
                if let Ok((_, v)) = crate::api::helpers::validate_defining_query(&effective_q) {
                    let label = match v {
                        'i' => "immutable",
                        's' => "stable",
                        _ => "volatile",
                    };
                    patterns.push(format!("volatility:{}", label));
                }

                let patterns_str = if patterns.is_empty() {
                    None
                } else {
                    Some(patterns.join("; "))
                };
                rows.push(("dvm_patterns".to_string(), false, patterns_str));
            }
            Err(e) => {
                rows.push((
                    "dvm_patterns".to_string(),
                    false,
                    Some(format!("dvm_parse_error: {}", e)),
                ));
            }
        }
    } else {
        rows.push((
            "dvm_patterns".to_string(),
            false,
            Some("ivm_support:TOPK".to_string()),
        ));
    }

    Ok(rows)
}

/// Recursively walk an `OpTree` and return a list of construct descriptors.
///
/// Returns strings of the form `"construct_name:detail"` for noteworthy
/// patterns (group-rescan aggregates, FULL JOINs, DISTINCT, EXCEPT/INTERSECT,
/// lateral subqueries, window functions, etc.).
fn collect_tree_constructs(tree: &OpTree, cte_registry: &dvm::CteRegistry) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    collect_tree_constructs_inner(tree, cte_registry, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_tree_constructs_inner(
    tree: &OpTree,
    cte_registry: &dvm::CteRegistry,
    out: &mut Vec<String>,
) {
    match tree {
        OpTree::Aggregate {
            aggregates, child, ..
        } => {
            for agg in aggregates {
                let strategy = dvm::classify_agg_strategy(agg);
                let label = agg_label(agg);
                out.push(format!("aggregate:{}({})", label, strategy));
            }
            collect_tree_constructs_inner(child, cte_registry, out);
        }
        OpTree::FullJoin { left, right, .. } => {
            out.push("join:FULL_OUTER".to_string());
            collect_tree_constructs_inner(left, cte_registry, out);
            collect_tree_constructs_inner(right, cte_registry, out);
        }
        OpTree::InnerJoin { left, right, .. } => {
            out.push("join:INNER".to_string());
            collect_tree_constructs_inner(left, cte_registry, out);
            collect_tree_constructs_inner(right, cte_registry, out);
        }
        OpTree::LeftJoin { left, right, .. } => {
            out.push("join:LEFT_OUTER".to_string());
            collect_tree_constructs_inner(left, cte_registry, out);
            collect_tree_constructs_inner(right, cte_registry, out);
        }
        OpTree::SemiJoin { left, right, .. } => {
            out.push("join:SEMI".to_string());
            collect_tree_constructs_inner(left, cte_registry, out);
            collect_tree_constructs_inner(right, cte_registry, out);
        }
        OpTree::AntiJoin { left, right, .. } => {
            out.push("join:ANTI".to_string());
            collect_tree_constructs_inner(left, cte_registry, out);
            collect_tree_constructs_inner(right, cte_registry, out);
        }
        OpTree::Distinct { child } => {
            out.push("set_op:DISTINCT".to_string());
            collect_tree_constructs_inner(child, cte_registry, out);
        }
        OpTree::UnionAll { children } => {
            out.push("set_op:UNION_ALL".to_string());
            for c in children {
                collect_tree_constructs_inner(c, cte_registry, out);
            }
        }
        OpTree::Intersect {
            left, right, all, ..
        } => {
            let label = if *all { "INTERSECT_ALL" } else { "INTERSECT" };
            out.push(format!("set_op:{}", label));
            collect_tree_constructs_inner(left, cte_registry, out);
            collect_tree_constructs_inner(right, cte_registry, out);
        }
        OpTree::Except {
            left, right, all, ..
        } => {
            let label = if *all { "EXCEPT_ALL" } else { "EXCEPT" };
            out.push(format!("set_op:{}", label));
            collect_tree_constructs_inner(left, cte_registry, out);
            collect_tree_constructs_inner(right, cte_registry, out);
        }
        OpTree::Window { child, .. } => {
            out.push("window_function:true".to_string());
            collect_tree_constructs_inner(child, cte_registry, out);
        }
        OpTree::LateralFunction { child, .. } => {
            out.push("lateral:FUNCTION".to_string());
            collect_tree_constructs_inner(child, cte_registry, out);
        }
        OpTree::LateralSubquery { child, .. } => {
            out.push("lateral:SUBQUERY".to_string());
            collect_tree_constructs_inner(child, cte_registry, out);
        }
        OpTree::ScalarSubquery { child, .. } => {
            out.push("scalar_subquery:true".to_string());
            collect_tree_constructs_inner(child, cte_registry, out);
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            out.push("recursive_cte:true".to_string());
            collect_tree_constructs_inner(base, cte_registry, out);
            collect_tree_constructs_inner(recursive, cte_registry, out);
        }
        OpTree::CteScan { cte_id, body, .. } => {
            if let Some(body) = body {
                collect_tree_constructs_inner(body, cte_registry, out);
            } else if let Some((_, body)) = cte_registry.get(*cte_id) {
                collect_tree_constructs_inner(body, cte_registry, out);
            }
        }
        OpTree::Project { child, .. }
        | OpTree::Filter { child, .. }
        | OpTree::Subquery { child, .. } => {
            collect_tree_constructs_inner(child, cte_registry, out);
        }
        OpTree::Scan { .. } | OpTree::RecursiveSelfRef { .. } | OpTree::ConstantSelect { .. } => {}
    }
}

/// Return a display label for an aggregate function (name + DISTINCT marker).
fn agg_label(agg: &AggExpr) -> String {
    use crate::dvm::parser::AggFunc;
    let name = match &agg.function {
        AggFunc::Count => "COUNT",
        AggFunc::CountStar => "COUNT(*)",
        AggFunc::Sum => "SUM",
        AggFunc::Avg => "AVG",
        AggFunc::Min => "MIN",
        AggFunc::Max => "MAX",
        AggFunc::BoolAnd => "BOOL_AND",
        AggFunc::BoolOr => "BOOL_OR",
        AggFunc::StringAgg => "STRING_AGG",
        AggFunc::ArrayAgg => "ARRAY_AGG",
        AggFunc::JsonAgg => "JSON_AGG",
        AggFunc::JsonbAgg => "JSONB_AGG",
        AggFunc::BitAnd => "BIT_AND",
        AggFunc::BitOr => "BIT_OR",
        AggFunc::BitXor => "BIT_XOR",
        AggFunc::StddevPop | AggFunc::StddevSamp => "STDDEV",
        AggFunc::VarPop | AggFunc::VarSamp => "VAR",
        AggFunc::Corr => "CORR",
        AggFunc::CovarPop | AggFunc::CovarSamp => "COVAR",
        AggFunc::XmlAgg => "XMLAGG",
        AggFunc::Mode => "MODE",
        AggFunc::PercentileCont => "PERCENTILE_CONT",
        AggFunc::PercentileDisc => "PERCENTILE_DISC",
        AggFunc::AnyValue => "ANY_VALUE",
        AggFunc::UserDefined(name) => name.as_str(),
        _ => "AGG",
    };
    if agg.is_distinct {
        format!("{} DISTINCT", name)
    } else {
        name.to_string()
    }
}

// ── DT-2: diagnose_errors ─────────────────────────────────────────────────

/// DT-2 — Return the last 5 FAILED refresh events for a stream table,
/// with each error classified by type and supplied with a remediation hint.
///
/// `name` accepts the same format as `create_stream_table`: either a
/// schema-qualified name (`'myschema.my_st'`) or an unqualified name
/// (`'my_st'`, resolved in `public`).
///
/// # Columns
/// - `event_time` — when the refresh started
/// - `error_type` — one of: `user`, `schema`, `correctness`,
///   `performance`, `infrastructure`
/// - `error_message` — the raw error text from `pgt_refresh_history`
/// - `remediation` — a suggested next step
///
/// # SQL usage
/// ```sql
/// SELECT * FROM pgtrickle.diagnose_errors('my_stream_table');
/// ```
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle")]
pub fn diagnose_errors(
    name: &str,
) -> TableIterator<
    'static,
    (
        name!(event_time, Option<TimestampWithTimeZone>),
        name!(error_type, String),
        name!(error_message, String),
        name!(remediation, String),
    ),
> {
    let rows = match diagnose_errors_impl(name) {
        Ok(r) => r,
        Err(e) => pgrx::error!("diagnose_errors: {}", e),
    };
    TableIterator::new(rows)
}

#[allow(clippy::type_complexity)]
fn diagnose_errors_impl(
    name: &str,
) -> Result<Vec<(Option<TimestampWithTimeZone>, String, String, String)>, PgTrickleError> {
    let (schema, table_name) = crate::api::parse_qualified_name_pub(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    let rows = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT h.start_time, h.error_message \
                 FROM pgtrickle.pgt_refresh_history h \
                 WHERE h.pgt_id = $1 AND h.status = 'FAILED' \
                 ORDER BY h.start_time DESC \
                 LIMIT 5",
                None,
                &[st.pgt_id.into()],
            )
            .map_err(|e| {
                PgTrickleError::SpiError(format!("diagnose_errors history query failed: {e}"))
            })?;

        let mut out = Vec::new();
        for row in result {
            let event_time = row.get::<TimestampWithTimeZone>(1).unwrap_or(None);
            let error_msg = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let (error_type, remediation) = classify_error(&error_msg);
            out.push((event_time, error_type, error_msg, remediation));
        }
        Ok(out)
    })?;

    Ok(rows)
}

/// Classify a raw error message string into a category with a remediation hint.
///
/// Categories:
/// - `"user"` — bad query, unsupported construct, type mismatch, invalid arg
/// - `"schema"` — upstream schema changed or table dropped
/// - `"correctness"` — phantom rows, EXCEPT ALL overflow, row-count mismatch
/// - `"performance"` — lock timeouts, serialization failures, I/O spills
/// - `"infrastructure"` — permission errors, SPI failures, slot errors
fn classify_error(msg: &str) -> (String, String) {
    let m = msg.to_lowercase();

    if m.contains("unsupported operator")
        || m.contains("query parse error")
        || m.contains("type mismatch")
        || m.contains("invalid argument")
        || m.contains("syntax error")
        || m.contains("unsupported construct")
    {
        return (
            "user".to_string(),
            "Check that the defining query uses only supported constructs. \
             Run pgtrickle.validate_query(query) for a detailed analysis, \
             or set refresh_mode = 'AUTO' to fall back to FULL refresh."
                .to_string(),
        );
    }

    if m.contains("schema changed")
        || m.contains("upstream table dropped")
        || m.contains("upstream table schema")
    {
        return (
            "schema".to_string(),
            "An upstream source table was altered or dropped. \
             Call pgtrickle.alter_stream_table('<name>') with the updated query \
             to reinitialize the stream table. Use pgtrickle.pgt_dependencies \
             to review all affected stream tables."
                .to_string(),
        );
    }

    if m.contains("phantom")
        || m.contains("except all")
        || m.contains("row count mismatch")
        || m.contains("multiplicity")
        || m.contains("correctness")
    {
        return (
            "correctness".to_string(),
            "A differential refresh produced an incorrect result. \
             This may indicate an EC-01 edge case with wide join trees. \
             Set refresh_mode = 'FULL' as a safe workaround and report \
             the query pattern to the pg_trickle issue tracker."
                .to_string(),
        );
    }

    if m.contains("lock timeout")
        || m.contains("deadlock")
        || m.contains("serialization failure")
        || m.contains("could not serialize")
        || m.contains("spill")
        || m.contains("temp file")
        || m.contains("out of memory")
    {
        return (
            "performance".to_string(),
            "The refresh encountered resource contention or excessive resource use. \
             Consider increasing pg_trickle.lock_timeout, reducing the schedule \
             frequency, or enabling buffer_partitioning to reduce change buffer size. \
             For EXCEPT_ALL spills, switch to FULL refresh mode."
                .to_string(),
        );
    }

    if m.contains("permission denied")
        || m.contains("spi permission")
        || m.contains("replication slot")
        || m.contains("wal")
        || m.contains("spi error")
        || m.contains("connection")
    {
        return (
            "infrastructure".to_string(),
            "A system-level failure occurred. Check that the background worker role \
             has SELECT on all source tables and INSERT/UPDATE/DELETE on the stream \
             table. For replication slot errors, verify pg_trickle.slot_name \
             and that max_replication_slots is not exhausted."
                .to_string(),
        );
    }

    // Default: infrastructure
    (
        "infrastructure".to_string(),
        "An unexpected error occurred during refresh. Check the PostgreSQL server log \
         for additional context. If this recurs, consider filing a bug report with \
         pgtrickle.explain_query_rewrite(query) and pgtrickle.validate_query(query) output."
            .to_string(),
    )
}

// ── DT-3: list_auxiliary_columns ─────────────────────────────────────────

/// DT-3 — List all `__pgt_*` internal columns on a stream table's storage
/// relation, with an explanation of each column's role.
///
/// These hidden columns are normally invisible in `SELECT *` output via
/// the `__pgt_*` column-exclusion filter. This function surfaces them for
/// debugging and operator visibility.
///
/// # Columns
/// - `column_name` — the `__pgt_*` column name (e.g. `__pgt_row_id`)
/// - `data_type` — PostgreSQL type name (e.g. `bigint`, `text`)
/// - `purpose` — human-readable description of what the column tracks
///
/// # SQL usage
/// ```sql
/// SELECT * FROM pgtrickle.list_auxiliary_columns('my_stream_table');
/// ```
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle")]
pub fn list_auxiliary_columns(
    name: &str,
) -> TableIterator<
    'static,
    (
        name!(column_name, String),
        name!(data_type, String),
        name!(purpose, String),
    ),
> {
    let rows = match list_auxiliary_columns_impl(name) {
        Ok(r) => r,
        Err(e) => pgrx::error!("list_auxiliary_columns: {}", e),
    };
    TableIterator::new(rows)
}

fn list_auxiliary_columns_impl(
    name: &str,
) -> Result<Vec<(String, String, String)>, PgTrickleError> {
    let (schema, table_name) = crate::api::parse_qualified_name_pub(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    let relid_i64: i64 = st.pgt_relid.to_u32() as i64;

    let rows = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT a.attname::text, \
                        pg_catalog.format_type(a.atttypid, a.atttypmod) \
                 FROM pg_catalog.pg_attribute a \
                 WHERE a.attrelid = $1::oid \
                   AND a.attname LIKE '__pgt_%' \
                   AND a.attnum > 0 \
                   AND NOT a.attisdropped \
                 ORDER BY a.attnum",
                None,
                &[relid_i64.into()],
            )
            .map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "list_auxiliary_columns attribute query failed: {e}"
                ))
            })?;

        let mut out = Vec::new();
        for row in result {
            let col_name = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let data_type = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let purpose = describe_aux_column(&col_name);
            out.push((col_name, data_type, purpose));
        }
        Ok(out)
    })?;

    Ok(rows)
}

/// Map a `__pgt_*` column name to a human-readable purpose string.
fn describe_aux_column(col: &str) -> String {
    if col == "__pgt_row_id" {
        return "Row identity tracking: a stable hash key uniquely identifying each \
                logical row. Used as the MERGE join key to apply delta changes \
                (INSERT / UPDATE / DELETE) to the storage table."
            .to_string();
    }
    if col == "__pgt_count" {
        return "Multiplicity counter: tracks how many times each logical row appears \
                in the result set. Required for queries with DISTINCT, aggregation, \
                UNION-dedup, or set-operations where a row can appear more than once."
            .to_string();
    }
    if col == "__pgt_count_l" {
        return "Left-side multiplicity counter: used together with __pgt_count_r for \
                INTERSECT / EXCEPT [ALL] to track left-branch occurrence counts \
                independently from right-branch counts."
            .to_string();
    }
    if col == "__pgt_count_r" {
        return "Right-side multiplicity counter: used together with __pgt_count_l for \
                INTERSECT / EXCEPT [ALL] to track right-branch occurrence counts \
                independently from left-branch counts."
            .to_string();
    }
    if col.starts_with("__pgt_aux_sum_") {
        let arg = col.trim_start_matches("__pgt_aux_sum_");
        return format!(
            "AVG auxiliary — running SUM of '{}': accumulates the sum component \
             so that algebraic AVG can be maintained as SUM / COUNT \
             without a full re-scan on each delta.",
            arg
        );
    }
    if col.starts_with("__pgt_aux_count_") {
        let arg = col.trim_start_matches("__pgt_aux_count_");
        return format!(
            "AVG auxiliary — running COUNT of non-NULL '{}': accumulates the \
             count component paired with __pgt_aux_sum_{} for algebraic AVG \
             maintenance.",
            arg, arg
        );
    }
    if col.starts_with("__pgt_aux_sum2_") {
        let arg = col.trim_start_matches("__pgt_aux_sum2_");
        return format!(
            "STDDEV/VAR auxiliary — running sum-of-squares of '{}': used with \
             the SUM and COUNT auxiliaries to maintain STDDEV_POP, STDDEV_SAMP, \
             VAR_POP, and VAR_SAMP algebraically.",
            arg
        );
    }
    if col.starts_with("__pgt_aux_sumx_") {
        let arg = col.trim_start_matches("__pgt_aux_sumx_");
        return format!(
            "CORR/COVAR/REGR auxiliary — running sum of x-argument '{}': \
             part of the five-column auxiliary set for algebraic regression \
             and correlation aggregate maintenance.",
            arg
        );
    }
    if col.starts_with("__pgt_aux_sumy_") {
        let arg = col.trim_start_matches("__pgt_aux_sumy_");
        return format!(
            "CORR/COVAR/REGR auxiliary — running sum of y-argument '{}': \
             part of the five-column auxiliary set for algebraic regression \
             and correlation aggregate maintenance.",
            arg
        );
    }
    if col.starts_with("__pgt_aux_sumxy_") {
        let arg = col.trim_start_matches("__pgt_aux_sumxy_");
        return format!(
            "CORR/COVAR/REGR auxiliary — running sum of x*y products for '{}': \
             part of the five-column auxiliary set for algebraic co-variance \
             and correlation maintenance.",
            arg
        );
    }
    if col.starts_with("__pgt_aux_sumx2_") {
        let arg = col.trim_start_matches("__pgt_aux_sumx2_");
        return format!(
            "CORR/COVAR/REGR auxiliary — running sum of x² for '{}': \
             part of the five-column auxiliary set used to compute regression \
             slope, intercept, and R².",
            arg
        );
    }
    if col.starts_with("__pgt_aux_sumy2_") {
        let arg = col.trim_start_matches("__pgt_aux_sumy2_");
        return format!(
            "CORR/COVAR/REGR auxiliary — running sum of y² for '{}': \
             part of the five-column auxiliary set used to compute regression \
             slope, intercept, and R².",
            arg
        );
    }
    if col.starts_with("__pgt_aux_nonnull_") {
        let arg = col.trim_start_matches("__pgt_aux_nonnull_");
        return format!(
            "SUM-above-FULL-JOIN auxiliary — running count of non-NULL '{}' values: \
             required when a SUM aggregate sits above a FULL OUTER JOIN child. \
             Prevents incorrect NULL propagation when one side of the join \
             produces no rows for a key.",
            arg
        );
    }
    // Unknown __pgt_* column — shouldn't happen in practice.
    format!(
        "Internal pg_trickle column '{}'. See the pg_trickle documentation \
         for the full list of auxiliary column roles.",
        col
    )
}

// ── DT-4: validate_query ──────────────────────────────────────────────────

/// DT-4 — Parse and validate a query through the DVM pipeline without
/// creating a stream table; return the resolved refresh mode, detected
/// SQL constructs, and any warnings.
///
/// # Columns
/// - `check_name` — name of the check or construct
/// - `result` — the resolved value or detected construct description
/// - `severity` — `'INFO'`, `'WARNING'`, or `'ERROR'`
///
/// The first row always has `check_name = 'resolved_refresh_mode'` with
/// the mode that would be assigned under `refresh_mode = 'AUTO'`
/// (`'DIFFERENTIAL'`, `'FULL'`, or `'TOPK'`).
///
/// # SQL usage
/// ```sql
/// SELECT * FROM pgtrickle.validate_query(
///   'SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id'
/// );
/// ```
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle")]
pub fn validate_query(
    query: &str,
) -> TableIterator<
    'static,
    (
        name!(check_name, String),
        name!(result, String),
        name!(severity, String),
    ),
> {
    let rows = match validate_query_impl(query) {
        Ok(r) => r,
        Err(e) => pgrx::error!("validate_query: {}", e),
    };
    TableIterator::new(rows)
}

fn validate_query_impl(query: &str) -> Result<Vec<(String, String, String)>, PgTrickleError> {
    let mut rows: Vec<(String, String, String)> = Vec::new();

    // Run the full rewrite pipeline (mirrors run_query_rewrite_pipeline in api.rs).
    let q = match dvm::rewrite_views_inline(query) {
        Ok(q) => q,
        Err(e) => {
            rows.push((
                "rewrite_error".to_string(),
                format!("{}", e),
                "ERROR".to_string(),
            ));
            return Ok(rows);
        }
    };

    let q = apply_rewrite_safe(&q, dvm::rewrite_nested_window_exprs, &mut rows);
    let q = apply_rewrite_safe(&q, dvm::rewrite_distinct_on, &mut rows);
    let q = apply_rewrite_safe(&q, dvm::rewrite_grouping_sets, &mut rows);
    let q = apply_rewrite_safe(&q, dvm::rewrite_scalar_subquery_in_where, &mut rows);
    let q = apply_rewrite_safe(&q, dvm::rewrite_correlated_scalar_in_select, &mut rows);

    let mut q = q;
    for _ in 0..3 {
        let prev = q.clone();
        let q2 = dvm::rewrite_demorgan_sublinks(&q)?;
        let q2 = dvm::rewrite_sublinks_in_or(&q2)?;
        if q2 == prev {
            break;
        }
        q = q2;
    }
    let q = apply_rewrite_safe(&q, dvm::rewrite_rows_from, &mut rows);

    // TopK detection.
    let topk = dvm::detect_topk_pattern(&q)?;
    let (effective_q, is_topk) = if let Some(ref info) = topk {
        rows.push((
            "topk_pattern".to_string(),
            format!(
                "LIMIT {}, ORDER BY: {}",
                info.limit_value, info.order_by_sql
            ),
            "INFO".to_string(),
        ));
        (info.base_query.clone(), true)
    } else {
        (q.clone(), false)
    };

    // Determine what refresh mode AUTO would assign.
    let resolved_mode = if is_topk {
        "TOPK".to_string()
    } else {
        resolve_auto_refresh_mode(&effective_q, &mut rows)
    };

    // Insert the resolved-mode row at position 0.
    rows.insert(
        0,
        (
            "resolved_refresh_mode".to_string(),
            resolved_mode,
            "INFO".to_string(),
        ),
    );

    // Full DVM parse for further construct analysis (skip on FULL-only).
    if !is_topk {
        match dvm::parse_defining_query_full(&effective_q) {
            Ok(result) => {
                // Emit parse warnings.
                for w in &result.warnings {
                    rows.push((
                        "parse_warning".to_string(),
                        w.clone(),
                        "WARNING".to_string(),
                    ));
                }

                // Recurse: report constructs.
                let constructs = collect_tree_constructs(&result.tree, &result.cte_registry);
                for c in constructs {
                    let (check, detail) = split_construct_label(&c);
                    let severity = construct_severity(&check, &detail);
                    rows.push((check, detail, severity));
                }

                // Recursion.
                if result.has_recursion {
                    rows.push((
                        "recursive_cte".to_string(),
                        "true".to_string(),
                        "WARNING".to_string(),
                    ));
                }

                // Volatility.
                if let Ok((_, v)) = crate::api::helpers::validate_defining_query(&effective_q) {
                    let (label, sev) = match v {
                        'i' => ("immutable", "INFO"),
                        's' => ("stable", "INFO"),
                        _ => ("volatile", "WARNING"),
                    };
                    rows.push(("volatility".to_string(), label.to_string(), sev.to_string()));
                }

                // Aux-column requirements.
                if dvm::query_needs_pgt_count(&effective_q) {
                    rows.push((
                        "needs_pgt_count".to_string(),
                        "true — multiplicity counter column required (DISTINCT/aggregation)"
                            .to_string(),
                        "INFO".to_string(),
                    ));
                }
                if dvm::query_needs_dual_count(&effective_q) {
                    rows.push((
                        "needs_dual_count".to_string(),
                        "true — left/right multiplicity counters required (INTERSECT/EXCEPT)"
                            .to_string(),
                        "INFO".to_string(),
                    ));
                }
            }
            Err(e) => {
                rows.push((
                    "dvm_parse_error".to_string(),
                    format!("{}", e),
                    "ERROR".to_string(),
                ));
            }
        }
    }

    Ok(rows)
}

/// Run a rewrite pass; on error push an ERROR row and return the input unchanged.
fn apply_rewrite_safe(
    q: &str,
    f: impl Fn(&str) -> Result<String, PgTrickleError>,
    rows: &mut Vec<(String, String, String)>,
) -> String {
    match f(q) {
        Ok(out) => out,
        Err(e) => {
            rows.push((
                "rewrite_error".to_string(),
                format!("{}", e),
                "ERROR".to_string(),
            ));
            q.to_string()
        }
    }
}

/// Run the checks that AUTO mode uses to decide whether DIFFERENTIAL is feasible.
/// Appends relevant WARNING rows and returns `"DIFFERENTIAL"` or `"FULL"`.
fn resolve_auto_refresh_mode(q: &str, rows: &mut Vec<(String, String, String)>) -> String {
    // 1. Non-linear / unsupported constructs.
    if let Err(e) = dvm::reject_unsupported_constructs(q) {
        rows.push((
            "unsupported_construct".to_string(),
            format!("{}", e),
            "WARNING".to_string(),
        ));
        return "FULL".to_string();
    }

    // 2. Materialized views / foreign tables.
    if let Err(e) = dvm::reject_materialized_views(q) {
        rows.push((
            "matview_or_foreign_table".to_string(),
            format!("{}", e),
            "WARNING".to_string(),
        ));
        return "FULL".to_string();
    }

    let volatility = match crate::api::helpers::validate_defining_query(q) {
        Ok((_, volatility)) => volatility,
        Err(e) => {
            rows.push((
                "query_analysis".to_string(),
                format!("query analysis failed: {e}"),
                "WARNING".to_string(),
            ));
            return "FULL".to_string();
        }
    };

    // 3. DVM parse + incremental admission.
    match dvm::parse_defining_query_full(q) {
        Ok(result) => match dvm::incremental_admission(&result, volatility, false) {
            Ok(dvm::IncrementalAdmission::Proven) => "DIFFERENTIAL".to_string(),
            Ok(dvm::IncrementalAdmission::FullOnly(issues)) => {
                for issue in issues {
                    rows.push((
                        issue.code.to_string(),
                        format!("{}: {}", issue.operator, issue.reason),
                        "WARNING".to_string(),
                    ));
                }
                "FULL".to_string()
            }
            Err(e) => {
                rows.push((
                    "incremental_admission".to_string(),
                    format!("inspection failed: {e}"),
                    "WARNING".to_string(),
                ));
                "FULL".to_string()
            }
        },
        Err(e) => {
            rows.push((
                "ivm_support_check".to_string(),
                format!("parse failed: {}", e),
                "WARNING".to_string(),
            ));
            "FULL".to_string()
        }
    }
}

/// Split a `"key:value"` construct label into `(key, value)`.
fn split_construct_label(label: &str) -> (String, String) {
    if let Some(pos) = label.find(':') {
        (label[..pos].to_string(), label[pos + 1..].to_string())
    } else {
        (label.to_string(), String::new())
    }
}

/// Return a severity string for a detected construct.
fn construct_severity(check: &str, detail: &str) -> String {
    match check {
        "join" if detail == "FULL_OUTER" => "WARNING",
        "set_op" if detail.starts_with("EXCEPT") => "WARNING",
        "window_function" => "INFO",
        "scalar_subquery" => "INFO",
        "lateral" => "INFO",
        "aggregate" if detail.contains("GROUP_RESCAN") => "WARNING",
        _ => "INFO",
    }
    .to_string()
}

// ── Shared helper re-exported for use by diagnostics from api.rs ──────────

// `parse_qualified_name` is defined in api.rs (not pub). We expose a thin
// wrapper here that diagnostics.rs can import via crate::api.
//
// NOTE: this wrapper is declared in api.rs as `pub(crate)` — see below.

// ── DIAG-1a: Refresh Mode Diagnostics — Pure Signal Scoring ───────────────

/// A scored diagnostic signal for refresh mode recommendation.
#[derive(Debug, Clone)]
pub struct Signal {
    /// Signal identifier (e.g. "change_ratio_current").
    pub name: &'static str,
    /// Score in [-1.0, +1.0]. Positive favors DIFFERENTIAL, negative favors FULL.
    pub score: f64,
    /// Effective weight after fallback adjustments.
    pub weight: f64,
    /// Original weight before fallback adjustments.
    pub base_weight: f64,
    /// Human-readable detail for this signal.
    pub detail: String,
}

/// A composite recommendation derived from multiple signals.
#[derive(Debug, Clone)]
pub struct Recommendation {
    /// "DIFFERENTIAL", "FULL", or "KEEP" (current config is near-optimal).
    pub recommended_mode: &'static str,
    /// "high", "medium", or "low".
    pub confidence: &'static str,
    /// Human-readable explanation highlighting the top signals.
    pub reason: String,
    /// The individual signals that contributed to this recommendation.
    pub signals: Vec<Signal>,
    /// The composite weighted score in [-1.0, +1.0].
    pub composite_score: f64,
}

/// Raw input data for the SPI layer to populate, consumed by signal scoring.
#[derive(Debug, Clone, Default)]
pub struct DiagnosticsInput {
    /// S1: Current change ratio (pending changes / source reltuples).
    pub change_ratio_current: Option<f64>,
    /// S2: Historical average change ratio from pgt_refresh_history.
    pub change_ratio_avg: Option<f64>,
    /// S2: Number of history rows used for the average.
    pub history_rows_total: i64,
    /// S3: Average DIFFERENTIAL refresh duration in ms.
    pub diff_avg_ms: Option<f64>,
    /// S3: Average FULL refresh duration in ms.
    pub full_avg_ms: Option<f64>,
    /// S3: Number of DIFFERENTIAL history rows.
    pub history_rows_diff: i64,
    /// S3: Number of FULL history rows.
    pub history_rows_full: i64,
    /// S4: Number of join nodes in the query OpTree.
    pub join_count: u32,
    /// S4: Maximum aggregate nesting depth.
    pub agg_depth: u32,
    /// S4: Whether the query uses window functions.
    pub has_window: bool,
    /// S4: Number of scalar/lateral subqueries.
    pub subquery_count: u32,
    /// S5: Target table size in bytes (relation + indexes).
    pub target_size_bytes: Option<i64>,
    /// S6: Whether a covering index exists on __pgt_row_id columns.
    pub has_covering_index: Option<bool>,
    /// S7: P95 latency for DIFFERENTIAL refreshes.
    pub diff_p95_ms: Option<f64>,
    /// S7: P50 (median) latency for DIFFERENTIAL refreshes.
    pub diff_p50_ms: Option<f64>,
    /// S7: Number of DIFFERENTIAL history rows used for latency stats.
    pub latency_history_rows: i64,
}

// ── Signal scoring functions (pure, no SPI) ────────────────────────────

/// S1/S2: Score a change ratio. Positive → favors DIFFERENTIAL, negative → favors FULL.
pub fn score_change_ratio(ratio: f64) -> f64 {
    if ratio < 0.05 {
        1.0
    } else if ratio < 0.15 {
        0.5
    } else if ratio < 0.30 {
        -0.3
    } else if ratio < 0.50 {
        -0.7
    } else {
        -1.0
    }
}

/// S3: Score empirical timing comparison (DIFF avg vs FULL avg).
pub fn score_empirical_timing(diff_avg_ms: f64, full_avg_ms: f64) -> f64 {
    if full_avg_ms <= 0.0 {
        return 0.0;
    }
    let ratio = diff_avg_ms / full_avg_ms;
    if ratio < 0.3 {
        1.0
    } else if ratio < 0.7 {
        0.5
    } else if ratio < 1.0 {
        0.2
    } else if ratio < 1.5 {
        -0.5
    } else {
        -1.0
    }
}

/// S4: Score query complexity based on OpTree characteristics.
pub fn score_query_complexity(
    join_count: u32,
    agg_depth: u32,
    has_window: bool,
    subquery_count: u32,
) -> f64 {
    let complexity = join_count + agg_depth + subquery_count + if has_window { 1 } else { 0 };
    if complexity == 0 {
        0.3 // simple scan/filter
    } else if complexity <= 2 {
        0.1 // moderate
    } else if complexity <= 4 {
        -0.2 // complex
    } else {
        -0.5 // very complex
    }
}

/// S5: Score target table size. Larger tables favor DIFFERENTIAL.
pub fn score_target_size(bytes: i64) -> f64 {
    const MB: i64 = 1_048_576;
    const GB: i64 = 1_073_741_824;
    if bytes < MB {
        -0.3
    } else if bytes < 100 * MB {
        0.1
    } else if bytes < GB {
        0.5
    } else {
        0.8
    }
}

/// S6: Score index coverage on target table.
pub fn score_index_coverage(has_covering: bool) -> f64 {
    if has_covering { 0.2 } else { -0.2 }
}

/// S7: Score latency variance (P95/P50 ratio for DIFFERENTIAL refreshes).
pub fn score_latency_variance(p95: f64, p50: f64) -> f64 {
    if p50 <= 0.0 {
        return 0.0;
    }
    let ratio = p95 / p50;
    if ratio < 2.0 {
        0.2
    } else if ratio < 5.0 {
        0.0
    } else {
        -0.3
    }
}

/// Collect all signals from a `DiagnosticsInput` and compute individual scores.
pub fn collect_signals(input: &DiagnosticsInput) -> Vec<Signal> {
    let mut signals = Vec::with_capacity(7);

    // S1: Change ratio (current)
    let (s1_score, s1_weight, s1_detail) = match input.change_ratio_current {
        Some(ratio) => (
            score_change_ratio(ratio),
            0.25,
            format!("current change ratio {:.3}", ratio),
        ),
        None => (0.0, 0.0, "no current change data".to_string()),
    };
    signals.push(Signal {
        name: "change_ratio_current",
        score: s1_score,
        weight: s1_weight,
        base_weight: 0.25,
        detail: s1_detail,
    });

    // S2: Historical change ratio
    let s2_base_weight = 0.30;
    let (s2_score, s2_weight, s2_detail) = match input.change_ratio_avg {
        Some(ratio) => {
            let w = if input.history_rows_total < 10 {
                0.10
            } else {
                s2_base_weight
            };
            (
                score_change_ratio(ratio),
                w,
                format!(
                    "avg change ratio {:.3} ({} history rows)",
                    ratio, input.history_rows_total
                ),
            )
        }
        None => (0.0, 0.0, "no refresh history".to_string()),
    };
    signals.push(Signal {
        name: "change_ratio_avg",
        score: s2_score,
        weight: s2_weight,
        base_weight: s2_base_weight,
        detail: s2_detail,
    });

    // S3: Empirical timing
    let s3_base_weight = 0.35;
    let (s3_score, s3_weight, s3_detail) = match (input.diff_avg_ms, input.full_avg_ms) {
        (Some(diff), Some(full))
            if input.history_rows_diff >= 5 && input.history_rows_full >= 5 =>
        {
            (
                score_empirical_timing(diff, full),
                s3_base_weight,
                format!("DIFF avg {:.1}ms vs FULL avg {:.1}ms", diff, full),
            )
        }
        _ => (
            0.0,
            0.0,
            "insufficient timing data (need ≥5 of each mode)".to_string(),
        ),
    };
    signals.push(Signal {
        name: "empirical_timing",
        score: s3_score,
        weight: s3_weight,
        base_weight: s3_base_weight,
        detail: s3_detail,
    });

    // S4: Query complexity
    let s4_score = score_query_complexity(
        input.join_count,
        input.agg_depth,
        input.has_window,
        input.subquery_count,
    );
    signals.push(Signal {
        name: "query_complexity",
        score: s4_score,
        weight: 0.10,
        base_weight: 0.10,
        detail: format!(
            "{} joins, agg depth {}, {} subqueries{}",
            input.join_count,
            input.agg_depth,
            input.subquery_count,
            if input.has_window { ", window fns" } else { "" }
        ),
    });

    // S5: Target size
    let (s5_score, s5_weight, s5_detail) = match input.target_size_bytes {
        Some(bytes) => (
            score_target_size(bytes),
            0.10,
            format!("target size {}", format_bytes(bytes)),
        ),
        None => (0.0, 0.0, "target size unknown".to_string()),
    };
    signals.push(Signal {
        name: "target_size",
        score: s5_score,
        weight: s5_weight,
        base_weight: 0.10,
        detail: s5_detail,
    });

    // S6: Index coverage
    let (s6_score, s6_weight, s6_detail) = match input.has_covering_index {
        Some(has) => (
            score_index_coverage(has),
            0.05,
            if has {
                "covering index on row-id columns".to_string()
            } else {
                "no covering index on row-id columns".to_string()
            },
        ),
        None => (0.0, 0.0, "index coverage unknown".to_string()),
    };
    signals.push(Signal {
        name: "index_coverage",
        score: s6_score,
        weight: s6_weight,
        base_weight: 0.05,
        detail: s6_detail,
    });

    // S7: Latency variance
    let s7_base_weight = 0.05;
    let (s7_score, s7_weight, s7_detail) = match (input.diff_p95_ms, input.diff_p50_ms) {
        (Some(p95), Some(p50)) if input.latency_history_rows >= 20 => (
            score_latency_variance(p95, p50),
            s7_base_weight,
            format!(
                "P95/P50 = {:.1}/{:.1} = {:.1}×",
                p95,
                p50,
                p95 / p50.max(0.001)
            ),
        ),
        _ => (
            0.0,
            0.0,
            "insufficient latency data (need ≥20 DIFF rows)".to_string(),
        ),
    };
    signals.push(Signal {
        name: "latency_variance",
        score: s7_score,
        weight: s7_weight,
        base_weight: s7_base_weight,
        detail: s7_detail,
    });

    signals
}

/// Compute a composite recommendation from collected signals.
pub fn compute_recommendation(signals: &[Signal], effective_mode: Option<&str>) -> Recommendation {
    let total_weight: f64 = signals.iter().map(|s| s.weight).sum();
    let max_weight: f64 = signals.iter().map(|s| s.base_weight).sum();

    let composite = if total_weight > 0.0 {
        signals.iter().map(|s| s.score * s.weight).sum::<f64>() / total_weight
    } else {
        0.0
    };

    // Confidence derivation
    let mut confidence = if max_weight > 0.0 && total_weight >= 0.80 * max_weight {
        "high"
    } else if max_weight > 0.0 && total_weight >= 0.50 * max_weight {
        "medium"
    } else {
        "low"
    };

    // Promote to at least "medium" if any heavy signal is strong
    if confidence == "low" {
        let has_strong = signals
            .iter()
            .any(|s| s.base_weight >= 0.25 && s.score.abs() >= 0.8);
        if has_strong {
            confidence = "medium";
        }
    }

    // Decision with dead zone ±0.15
    let raw_mode = if composite > 0.15 {
        "DIFFERENTIAL"
    } else if composite < -0.15 {
        "FULL"
    } else {
        "KEEP"
    };

    // Don't recommend what they already effectively use
    let recommended = if let Some(eff) = effective_mode {
        if raw_mode == eff { "KEEP" } else { raw_mode }
    } else {
        raw_mode
    };

    // Format reason: highlight top 2 signals by |score × weight|
    let mut ranked: Vec<_> = signals.iter().filter(|s| s.weight > 0.0).collect();
    ranked.sort_by(|a, b| {
        let wa = (a.score * a.weight).abs();
        let wb = (b.score * b.weight).abs();
        wb.partial_cmp(&wa).unwrap_or(std::cmp::Ordering::Equal)
    });

    let reason = if ranked.is_empty() {
        "no diagnostic data available".to_string()
    } else {
        let top: Vec<String> = ranked.iter().take(2).map(|s| s.detail.clone()).collect();
        top.join("; ")
    };

    Recommendation {
        recommended_mode: recommended,
        confidence,
        reason,
        signals: signals.to_vec(),
        composite_score: composite,
    }
}

/// Format bytes as a human-readable string.
fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1_048_576;
    const GB: i64 = 1_073_741_824;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

// ── DIAG-1b: SPI Data-Gathering Layer ─────────────────────────────────────

/// History statistics gathered from pgt_refresh_history via SPI.
#[derive(Debug, Clone, Default)]
pub struct HistoryStats {
    pub avg_change_ratio: Option<f64>,
    pub total_rows: i64,
    pub diff_avg_ms: Option<f64>,
    pub full_avg_ms: Option<f64>,
    pub diff_count: i64,
    pub full_count: i64,
    pub diff_p95_ms: Option<f64>,
    pub diff_p50_ms: Option<f64>,
    pub diff_latency_rows: i64,
}

/// Gather current change ratio for a stream table's source tables.
pub fn gather_change_ratio(st: &StreamTableMeta) -> Option<f64> {
    let change_schema = crate::config::pg_trickle_change_buffer_schema();
    let deps = crate::catalog::StDependency::get_for_st(st.pgt_id).unwrap_or_default();

    let mut total_changes: i64 = 0;
    let mut total_reltuples: f64 = 0.0;

    for dep in &deps {
        if dep.source_type != "TABLE" && dep.source_type != "FOREIGN_TABLE" {
            continue;
        }
        let oid = dep.source_relid.to_u32();

        // Count pending change buffer rows (v0.32.0+: stable buffer name)
        let buf =
            crate::cdc::buffer_qualified_name_for_oid(&change_schema, pgrx::pg_sys::Oid::from(oid));
        // SAFETY: `buf` is constructed by buffer_qualified_name_for_oid from a
        // PostgreSQL OID — it is never user input. PostgreSQL does not allow bind
        // parameters as FROM-clause table references, so format! is required here.
        let changes = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {buf}")) // nosemgrep: rust.spi.query.dynamic-format
            .unwrap_or(Some(0))
            .unwrap_or(0);
        total_changes += changes;

        // Get source table reltuples estimate
        let tuples = Spi::get_one_with_args::<f64>(
            "SELECT GREATEST(reltuples, 1) FROM pg_class WHERE oid = $1::oid",
            &[(oid as i64).into()],
        )
        .unwrap_or(Some(1.0))
        .unwrap_or(1.0);
        total_reltuples += tuples;
    }

    if total_reltuples > 0.0 {
        Some(total_changes as f64 / total_reltuples)
    } else {
        None
    }
}

/// Gather refresh history statistics for a stream table.
#[allow(clippy::field_reassign_with_default)]
pub fn gather_history_stats(pgt_id: i64, target_relid: pg_sys::Oid) -> HistoryStats {
    let mut stats = HistoryStats::default();

    // Convert OID to i64 for SPI parameter passing (pgrx requires this to
    // avoid a silent type mismatch that causes `WHERE oid = $param` to return
    // NULL, collapsing the denominator to 1.0 and producing ratio ≈ 1.0).
    let target_relid_i64: i64 = target_relid.to_u32() as i64;

    // Total history row count
    stats.total_rows = Spi::get_one_with_args::<i64>(
        "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = $1 AND status = 'COMPLETED'",
        &[pgt_id.into()],
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);

    // Average change ratio from history: fraction of output rows that change
    // per differential refresh.  We divide by the stream table's current
    // reltuples so the ratio is meaningful (0 = nothing changes, 1 = every row
    // changes every refresh).  The old denominator (delta_row_count =
    // rows_inserted + rows_deleted) was a tautology — always 1.0.
    stats.avg_change_ratio = Spi::get_one_with_args::<f64>(
        "SELECT AVG(CASE WHEN (rows_inserted + rows_deleted) > 0 THEN \
           (rows_inserted + rows_deleted)::float / \
           GREATEST((SELECT GREATEST(reltuples, 1.0) \
                     FROM pg_class WHERE oid = $2), 1.0) \
           ELSE NULL END) \
         FROM (SELECT rows_inserted, rows_deleted \
               FROM pgtrickle.pgt_refresh_history \
               WHERE pgt_id = $1 AND status = 'COMPLETED' \
               ORDER BY start_time DESC LIMIT 100) sub",
        &[pgt_id.into(), target_relid_i64.into()],
    )
    .unwrap_or(None);

    // DIFFERENTIAL timing stats
    stats.diff_count = Spi::get_one_with_args::<i64>(
        "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = $1 AND action = 'DIFFERENTIAL' AND status = 'COMPLETED'",
        &[pgt_id.into()],
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);

    if stats.diff_count > 0 {
        stats.diff_avg_ms = Spi::get_one_with_args::<f64>(
            "SELECT AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000) \
             FROM (SELECT start_time, end_time FROM pgtrickle.pgt_refresh_history \
                   WHERE pgt_id = $1 AND action = 'DIFFERENTIAL' AND status = 'COMPLETED' \
                     AND end_time IS NOT NULL \
                   ORDER BY start_time DESC LIMIT 100) sub",
            &[pgt_id.into()],
        )
        .unwrap_or(None);
    }

    // FULL timing stats
    stats.full_count = Spi::get_one_with_args::<i64>(
        "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = $1 AND action = 'FULL' AND status = 'COMPLETED'",
        &[pgt_id.into()],
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);

    if stats.full_count > 0 {
        stats.full_avg_ms = Spi::get_one_with_args::<f64>(
            "SELECT AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000) \
             FROM (SELECT start_time, end_time FROM pgtrickle.pgt_refresh_history \
                   WHERE pgt_id = $1 AND action = 'FULL' AND status = 'COMPLETED' \
                     AND end_time IS NOT NULL \
                   ORDER BY start_time DESC LIMIT 100) sub",
            &[pgt_id.into()],
        )
        .unwrap_or(None);
    }

    // Latency percentiles for DIFFERENTIAL
    stats.diff_latency_rows = std::cmp::min(stats.diff_count, 100);
    if stats.diff_latency_rows >= 20 {
        stats.diff_p50_ms = Spi::get_one_with_args::<f64>(
            "SELECT PERCENTILE_CONT(0.50) WITHIN GROUP \
                 (ORDER BY EXTRACT(EPOCH FROM (end_time - start_time)) * 1000) \
             FROM (SELECT start_time, end_time FROM pgtrickle.pgt_refresh_history \
                   WHERE pgt_id = $1 AND action = 'DIFFERENTIAL' AND status = 'COMPLETED' \
                     AND end_time IS NOT NULL \
                   ORDER BY start_time DESC LIMIT 100) sub",
            &[pgt_id.into()],
        )
        .unwrap_or(None);

        stats.diff_p95_ms = Spi::get_one_with_args::<f64>(
            "SELECT PERCENTILE_CONT(0.95) WITHIN GROUP \
                 (ORDER BY EXTRACT(EPOCH FROM (end_time - start_time)) * 1000) \
             FROM (SELECT start_time, end_time FROM pgtrickle.pgt_refresh_history \
                   WHERE pgt_id = $1 AND action = 'DIFFERENTIAL' AND status = 'COMPLETED' \
                     AND end_time IS NOT NULL \
                   ORDER BY start_time DESC LIMIT 100) sub",
            &[pgt_id.into()],
        )
        .unwrap_or(None);
    }

    stats
}

/// Gather query complexity stats by parsing the defining query through the DVM parser.
pub fn gather_query_complexity(st: &StreamTableMeta) -> (u32, u32, bool, u32) {
    match dvm::parser::parse_defining_query(&st.defining_query) {
        Ok(tree) => count_complexity(&tree),
        Err(_) => (0, 0, false, 0),
    }
}

/// Recursively count complexity metrics from an OpTree.
fn count_complexity(tree: &OpTree) -> (u32, u32, bool, u32) {
    let mut joins = 0u32;
    let mut agg_depth = 0u32;
    let mut has_window = false;
    let mut subqueries = 0u32;

    match tree {
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            joins += 1;
            let (lj, la, lw, ls) = count_complexity(left);
            let (rj, ra, rw, rs) = count_complexity(right);
            joins += lj + rj;
            agg_depth = agg_depth.max(la).max(ra);
            has_window = has_window || lw || rw;
            subqueries += ls + rs;
        }
        OpTree::Aggregate { child, .. } => {
            agg_depth += 1;
            let (cj, ca, cw, cs) = count_complexity(child);
            joins += cj;
            agg_depth += ca;
            has_window = has_window || cw;
            subqueries += cs;
        }
        OpTree::Project { child, .. } => {
            let (cj, ca, cw, cs) = count_complexity(child);
            joins += cj;
            agg_depth = agg_depth.max(ca);
            has_window = has_window || cw;
            subqueries += cs;
        }
        OpTree::Window { child, .. } => {
            has_window = true;
            let (cj, ca, cw, cs) = count_complexity(child);
            joins += cj;
            agg_depth = agg_depth.max(ca);
            has_window = has_window || cw;
            subqueries += cs;
        }
        OpTree::Subquery { child, .. } => {
            subqueries += 1;
            let (cj, ca, cw, cs) = count_complexity(child);
            joins += cj;
            agg_depth = agg_depth.max(ca);
            has_window = has_window || cw;
            subqueries += cs;
        }
        OpTree::Filter { child, .. } | OpTree::Distinct { child, .. } => {
            let (cj, ca, cw, cs) = count_complexity(child);
            joins += cj;
            agg_depth = agg_depth.max(ca);
            has_window = has_window || cw;
            subqueries += cs;
        }
        OpTree::UnionAll { children } => {
            for c in children {
                let (cj, ca, cw, cs) = count_complexity(c);
                joins += cj;
                agg_depth = agg_depth.max(ca);
                has_window = has_window || cw;
                subqueries += cs;
            }
        }
        OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
            let (lj, la, lw, ls) = count_complexity(left);
            let (rj, ra, rw, rs) = count_complexity(right);
            joins += lj + rj;
            agg_depth = agg_depth.max(la).max(ra);
            has_window = has_window || lw || rw;
            subqueries += ls + rs;
        }
        OpTree::SemiJoin { left, right, .. } | OpTree::AntiJoin { left, right, .. } => {
            joins += 1;
            let (lj, la, lw, ls) = count_complexity(left);
            let (rj, ra, rw, rs) = count_complexity(right);
            joins += lj + rj;
            agg_depth = agg_depth.max(la).max(ra);
            has_window = has_window || lw || rw;
            subqueries += ls + rs;
        }
        OpTree::LateralFunction { child, .. } | OpTree::LateralSubquery { child, .. } => {
            subqueries += 1;
            let (cj, ca, cw, cs) = count_complexity(child);
            joins += cj;
            agg_depth = agg_depth.max(ca);
            has_window = has_window || cw;
            subqueries += cs;
        }
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => {
            subqueries += 1;
            let (cj, ca, cw, cs) = count_complexity(child);
            let (sj, sa, sw, ss) = count_complexity(subquery);
            joins += cj + sj;
            agg_depth = agg_depth.max(ca).max(sa);
            has_window = has_window || cw || sw;
            subqueries += cs + ss;
        }
        OpTree::Scan { .. } | OpTree::CteScan { .. } => {}
        // Handle remaining variants that have a child
        _ => {}
    }

    (joins, agg_depth, has_window, subqueries)
}

/// Gather target table size (relation + indexes) in bytes.
pub fn gather_target_size(target_relid: pg_sys::Oid) -> Option<i64> {
    Spi::get_one_with_args::<i64>(
        "SELECT pg_relation_size($1) + pg_indexes_size($1)",
        &[target_relid.into()],
    )
    .unwrap_or(None)
}

/// Check if a covering index exists on __pgt_row_id columns of the target table.
pub fn gather_index_coverage(target_relid: pg_sys::Oid) -> Option<bool> {
    // Check whether any index on the target table covers the __pgt_row_id column
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS( \
           SELECT 1 FROM pg_index i \
           JOIN pg_attribute a ON a.attrelid = i.indrelid \
                AND a.attnum = ANY(i.indkey) \
           WHERE i.indrelid = $1 \
             AND a.attname = '__pgt_row_id' \
         )",
        &[target_relid.into()],
    )
    .unwrap_or(Some(false))
}

/// Build a complete DiagnosticsInput for a stream table by gathering all signals.
pub fn gather_all_signals(st: &StreamTableMeta) -> DiagnosticsInput {
    let change_ratio_current = gather_change_ratio(st);
    let history = gather_history_stats(st.pgt_id, st.pgt_relid);
    let (join_count, agg_depth, has_window, subquery_count) = gather_query_complexity(st);
    let target_size_bytes = gather_target_size(st.pgt_relid);
    let has_covering_index = gather_index_coverage(st.pgt_relid);

    DiagnosticsInput {
        change_ratio_current,
        change_ratio_avg: history.avg_change_ratio,
        history_rows_total: history.total_rows,
        diff_avg_ms: history.diff_avg_ms,
        full_avg_ms: history.full_avg_ms,
        history_rows_diff: history.diff_count,
        history_rows_full: history.full_count,
        join_count,
        agg_depth,
        has_window,
        subquery_count,
        target_size_bytes,
        has_covering_index,
        diff_p95_ms: history.diff_p95_ms,
        diff_p50_ms: history.diff_p50_ms,
        latency_history_rows: history.diff_latency_rows,
    }
}

// ── DIAG-1a Unit Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_strategy_state_requirement_is_closed() {
        let eligible = serde_json::json!({
            "nodes": [{"functions": [{"eligible": true, "runtime_enabled": true}]}]
        });
        let fallback = serde_json::json!({
            "nodes": [{"functions": [{"eligible": true, "runtime_enabled": false}]}]
        });
        assert!(strategy_requires_state(Some(&eligible)));
        assert!(!strategy_requires_state(Some(&fallback)));
        assert!(!strategy_requires_state(Some(&serde_json::json!({
            "schema_version": 999
        }))));
    }

    #[test]
    fn test_window_runtime_evidence_distinguishes_partition_recompute() {
        let (strategy, emitted, crossover) = window_runtime_evidence(
            Some("DIFFERENTIAL"),
            Some("WINDOW_RECOMPUTE_CHEAPER"),
            Some(
                r#"{"strategy":"mixed","estimated_emitted_rows":42,"crossover_evidence":{"version":1}}"#,
            ),
            true,
        );
        assert_eq!(strategy.as_deref(), Some("mixed"));
        assert_eq!(emitted, Some(42));
        assert_eq!(crossover, Some(serde_json::json!({"version": 1})));

        let (strategy, _, _) = window_runtime_evidence(
            Some("DIFFERENTIAL"),
            Some("WINDOW_UNSUPPORTED_FRAME"),
            None,
            true,
        );
        assert_eq!(strategy.as_deref(), Some("partition_recompute"));

        let (strategy, _, _) = window_runtime_evidence(
            Some("IMMEDIATE"),
            Some("WINDOW_IMMEDIATE_RECOMPUTE"),
            None,
            false,
        );
        assert_eq!(strategy.as_deref(), Some("immediate_recompute"));
    }

    #[test]
    fn test_window_strategy_summary_includes_static_fallback() {
        let strategy = serde_json::json!({
            "nodes": [{
                "functions": [
                    {"kind": "row_number", "strategy": "ordered_suffix", "fallback_reason": null},
                    {"kind": "lag", "strategy": "partition_recompute", "fallback_reason": "WINDOW_UNSUPPORTED_ARGUMENT"}
                ]
            }]
        });
        assert_eq!(
            summarize_window_strategy(Some(&strategy)),
            "row_number=ordered_suffix, lag=partition_recompute (WINDOW_UNSUPPORTED_ARGUMENT)"
        );
    }

    #[test]
    fn test_score_change_ratio_boundaries() {
        assert_eq!(score_change_ratio(0.0), 1.0);
        assert_eq!(score_change_ratio(0.04), 1.0);
        assert_eq!(score_change_ratio(0.05), 0.5);
        assert_eq!(score_change_ratio(0.14), 0.5);
        assert_eq!(score_change_ratio(0.15), -0.3);
        assert_eq!(score_change_ratio(0.29), -0.3);
        assert_eq!(score_change_ratio(0.30), -0.7);
        assert_eq!(score_change_ratio(0.49), -0.7);
        assert_eq!(score_change_ratio(0.50), -1.0);
        assert_eq!(score_change_ratio(1.0), -1.0);
    }

    #[test]
    fn test_score_empirical_timing_boundaries() {
        // DIFF much faster → +1.0
        assert_eq!(score_empirical_timing(10.0, 100.0), 1.0);
        // DIFF faster → +0.5
        assert_eq!(score_empirical_timing(50.0, 100.0), 0.5);
        // DIFF slightly faster → +0.2
        assert_eq!(score_empirical_timing(80.0, 100.0), 0.2);
        // DIFF slower → -0.5
        assert_eq!(score_empirical_timing(120.0, 100.0), -0.5);
        // DIFF much slower → -1.0
        assert_eq!(score_empirical_timing(200.0, 100.0), -1.0);
        // Zero full_avg → 0.0
        assert_eq!(score_empirical_timing(10.0, 0.0), 0.0);
    }

    #[test]
    fn test_score_query_complexity_levels() {
        // Simple: scan only
        assert_eq!(score_query_complexity(0, 0, false, 0), 0.3);
        // Moderate: 1 join + 1 agg
        assert_eq!(score_query_complexity(1, 1, false, 0), 0.1);
        // Complex: 3 joins
        assert_eq!(score_query_complexity(3, 0, false, 0), -0.2);
        // Very complex: 5 joins + window + subquery
        assert_eq!(score_query_complexity(5, 0, true, 1), -0.5);
    }

    #[test]
    fn test_score_target_size_range() {
        assert_eq!(score_target_size(100_000), -0.3); // 100KB
        assert_eq!(score_target_size(10_000_000), 0.1); // 10MB
        assert_eq!(score_target_size(500_000_000), 0.5); // 500MB
        assert_eq!(score_target_size(2_000_000_000), 0.8); // 2GB
    }

    #[test]
    fn test_score_index_coverage() {
        assert_eq!(score_index_coverage(true), 0.2);
        assert_eq!(score_index_coverage(false), -0.2);
    }

    #[test]
    fn test_score_latency_variance() {
        assert_eq!(score_latency_variance(15.0, 10.0), 0.2); // 1.5×
        assert_eq!(score_latency_variance(30.0, 10.0), 0.0); // 3×
        assert_eq!(score_latency_variance(60.0, 10.0), -0.3); // 6×
        assert_eq!(score_latency_variance(10.0, 0.0), 0.0); // zero median
    }

    #[test]
    fn test_composite_all_favor_diff() {
        let input = DiagnosticsInput {
            change_ratio_current: Some(0.01),
            change_ratio_avg: Some(0.02),
            history_rows_total: 100,
            diff_avg_ms: Some(10.0),
            full_avg_ms: Some(300.0),
            history_rows_diff: 80,
            history_rows_full: 20,
            join_count: 0,
            agg_depth: 0,
            has_window: false,
            subquery_count: 0,
            target_size_bytes: Some(500_000_000),
            has_covering_index: Some(true),
            diff_p95_ms: Some(15.0),
            diff_p50_ms: Some(10.0),
            latency_history_rows: 80,
        };
        let signals = collect_signals(&input);
        let rec = compute_recommendation(&signals, Some("FULL"));
        assert_eq!(rec.recommended_mode, "DIFFERENTIAL");
        assert_eq!(rec.confidence, "high");
        assert!(rec.composite_score > 0.5);
    }

    #[test]
    fn test_composite_all_favor_full() {
        let input = DiagnosticsInput {
            change_ratio_current: Some(0.60),
            change_ratio_avg: Some(0.55),
            history_rows_total: 50,
            diff_avg_ms: Some(800.0),
            full_avg_ms: Some(300.0),
            history_rows_diff: 30,
            history_rows_full: 20,
            join_count: 4,
            agg_depth: 2,
            has_window: true,
            subquery_count: 1,
            target_size_bytes: Some(500_000),
            has_covering_index: Some(false),
            diff_p95_ms: Some(2000.0),
            diff_p50_ms: Some(300.0),
            latency_history_rows: 30,
        };
        let signals = collect_signals(&input);
        let rec = compute_recommendation(&signals, Some("DIFFERENTIAL"));
        assert_eq!(rec.recommended_mode, "FULL");
        assert_eq!(rec.confidence, "high");
        assert!(rec.composite_score < -0.5);
    }

    #[test]
    fn test_composite_dead_zone_returns_keep() {
        let input = DiagnosticsInput {
            change_ratio_current: Some(0.10),
            change_ratio_avg: Some(0.12),
            history_rows_total: 50,
            diff_avg_ms: Some(90.0),
            full_avg_ms: Some(100.0),
            history_rows_diff: 30,
            history_rows_full: 20,
            join_count: 1,
            agg_depth: 1,
            has_window: false,
            subquery_count: 0,
            target_size_bytes: Some(50_000_000),
            has_covering_index: Some(true),
            diff_p95_ms: Some(200.0),
            diff_p50_ms: Some(80.0),
            latency_history_rows: 30,
        };
        let signals = collect_signals(&input);
        let rec = compute_recommendation(&signals, Some("DIFFERENTIAL"));
        // Score should be close to 0, within dead zone
        assert_eq!(rec.recommended_mode, "KEEP");
    }

    #[test]
    fn test_confidence_low_with_no_data() {
        let input = DiagnosticsInput::default();
        let signals = collect_signals(&input);
        let rec = compute_recommendation(&signals, None);
        assert_eq!(rec.confidence, "low");
        // S4 (query_complexity) defaults to "simple" (score=0.3) even with
        // no data, so composite > 0.15 → DIFFERENTIAL recommendation. This
        // is correct: the only active signal says simple = favor DIFF.
    }

    #[test]
    fn test_confidence_medium_with_sparse_history() {
        let input = DiagnosticsInput {
            change_ratio_current: Some(0.02),
            change_ratio_avg: Some(0.03),
            history_rows_total: 5, // <10, weight reduced
            join_count: 0,
            agg_depth: 0,
            has_window: false,
            subquery_count: 0,
            ..Default::default()
        };
        let signals = collect_signals(&input);
        let rec = compute_recommendation(&signals, Some("FULL"));
        // Strong S1 signal promotes to at least medium
        assert!(rec.confidence == "medium" || rec.confidence == "high");
    }

    #[test]
    fn test_keep_when_already_optimal() {
        let input = DiagnosticsInput {
            change_ratio_current: Some(0.01),
            change_ratio_avg: Some(0.02),
            history_rows_total: 100,
            diff_avg_ms: Some(10.0),
            full_avg_ms: Some(300.0),
            history_rows_diff: 80,
            history_rows_full: 20,
            join_count: 0,
            agg_depth: 0,
            has_window: false,
            subquery_count: 0,
            target_size_bytes: Some(500_000_000),
            has_covering_index: Some(true),
            diff_p95_ms: Some(15.0),
            diff_p50_ms: Some(10.0),
            latency_history_rows: 80,
        };
        let signals = collect_signals(&input);
        // Already on DIFFERENTIAL — should return KEEP
        let rec = compute_recommendation(&signals, Some("DIFFERENTIAL"));
        assert_eq!(rec.recommended_mode, "KEEP");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(10_240), "10.0 KB");
        assert_eq!(format_bytes(52_428_800), "50.0 MB");
        assert_eq!(format_bytes(2_147_483_648), "2.0 GB");
    }

    // ── TEST-2: Additional unit tests for diagnostics.rs ──────────────

    #[test]
    fn test_classify_error_user_unsupported() {
        let (cat, _) = classify_error("unsupported operator: EXCEPT ALL");
        assert_eq!(cat, "user");
    }

    #[test]
    fn test_classify_error_schema_changed() {
        let (cat, _) = classify_error("upstream table schema changed: OID 12345");
        assert_eq!(cat, "schema");
    }

    #[test]
    fn test_classify_error_correctness_phantom() {
        let (cat, _) = classify_error("phantom rows detected after refresh cycle");
        assert_eq!(cat, "correctness");
    }

    #[test]
    fn test_classify_error_performance_lock_timeout() {
        let (cat, _) = classify_error("lock timeout while waiting for stream table refresh");
        assert_eq!(cat, "performance");
    }

    #[test]
    fn test_classify_error_infrastructure_permission() {
        let (cat, _) = classify_error("permission denied for table orders");
        assert_eq!(cat, "infrastructure");
    }

    #[test]
    fn test_classify_error_infrastructure_default() {
        // An unknown error → falls through to infrastructure category
        let (cat, _) = classify_error("something completely unknown happened");
        assert_eq!(cat, "infrastructure");
    }

    #[test]
    fn test_classify_error_is_case_insensitive() {
        // classify_error lowercases internally, so upper-case input should match
        let (cat, _) = classify_error("QUERY PARSE ERROR: unexpected token");
        assert_eq!(cat, "user");
    }

    #[test]
    fn test_compute_recommendation_already_on_correct_mode() {
        // Low change ratio + fast diff → KEEP if already on DIFFERENTIAL
        let input = DiagnosticsInput {
            change_ratio_current: Some(0.01),
            change_ratio_avg: Some(0.02),
            history_rows_total: 100,
            diff_avg_ms: Some(10.0),
            full_avg_ms: Some(300.0),
            history_rows_diff: 80,
            history_rows_full: 20,
            join_count: 0,
            agg_depth: 0,
            has_window: false,
            subquery_count: 0,
            target_size_bytes: Some(500_000_000),
            has_covering_index: Some(true),
            diff_p95_ms: Some(15.0),
            diff_p50_ms: Some(10.0),
            latency_history_rows: 80,
        };
        let signals = collect_signals(&input);
        // Already on DIFFERENTIAL — should return KEEP
        let rec = compute_recommendation(&signals, Some("DIFFERENTIAL"));
        assert_eq!(rec.recommended_mode, "KEEP");
    }
}
