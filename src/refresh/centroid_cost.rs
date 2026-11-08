//! Conservative strategy selection from recent comparable centroid refreshes.

use pgrx::prelude::*;
use serde_json::Value;

const HISTORY_LIMIT: i64 = 64;
const MIN_SAMPLES: usize = 3;
const COOLDOWN_REFRESHES: usize = 8;

pub(crate) const RESCAN: &str = "COST_MODEL_RESCAN";
pub(crate) const RESCAN_ACTIVE: &str = "COST_MODEL_RESCAN_ACTIVE";
pub(crate) const REBIND: &str = "COST_MODEL_REBIND";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Preference {
    Algebraic,
    Rescan,
    Unknown,
}

#[derive(Debug)]
struct Observation {
    completed: bool,
    action: String,
    strategy: String,
    detail: Option<Value>,
    elapsed_ms: Option<f64>,
}

impl Observation {
    fn reason(&self) -> Option<&str> {
        self.detail.as_ref()?["centroid_fallback"]["reason"].as_str()
    }

    fn data_change(&self) -> bool {
        self.completed
            && self
                .detail
                .as_ref()
                .and_then(|detail| detail["input_delta_rows"].as_i64())
                .is_some_and(|rows| rows > 0)
    }

    fn matches_contract(&self, plan: i64, generation: i64) -> bool {
        self.detail.as_ref().is_some_and(|detail| {
            detail["schema_version"].as_i64() == Some(1)
                && detail["plan_identity"].as_i64() == Some(plan)
                && detail["contract_generation"].as_i64() == Some(generation)
        })
    }
}

pub(crate) struct CostHistory {
    observations: Vec<Observation>,
}

impl CostHistory {
    pub(crate) fn window_full(&self) -> bool {
        self.observations.len() >= HISTORY_LIMIT as usize
    }

    pub(crate) fn in_cooldown(&self, plan: i64, generation: i64) -> bool {
        self.observations
            .iter()
            .filter(|observation| observation.data_change())
            .take(COOLDOWN_REFRESHES)
            .any(|observation| {
                observation.matches_contract(plan, generation)
                    && matches!(observation.reason(), Some(RESCAN | REBIND))
            })
    }

    pub(crate) fn preference(
        &self,
        plan: i64,
        generation: i64,
        input_rows: i64,
        source_rows: i64,
    ) -> Preference {
        let mut algebraic = Vec::new();
        let mut rescan = Vec::new();
        for observation in &self.observations {
            let Some(detail) = observation.detail.as_ref() else {
                continue;
            };
            let Some(elapsed_ms) = observation
                .elapsed_ms
                .filter(|ms| ms.is_finite() && *ms > 0.0)
            else {
                continue;
            };
            if !observation.completed
                || observation.action != "DIFFERENTIAL"
                || !observation.matches_contract(plan, generation)
                || detail["input_delta_rows_may_be_incomplete"].as_bool() != Some(false)
                || detail["input_delta_rows"].as_i64() != Some(input_rows)
                || detail["source_rows_estimate"].as_i64() != Some(source_rows)
                || detail["output_rows_changed"]
                    .as_i64()
                    .is_none_or(|rows| rows <= 0)
            {
                continue;
            }
            match observation.strategy.as_str() {
                "centroid_algebraic" if observation.reason().is_none() => {
                    algebraic.push(elapsed_ms)
                }
                "merge" | "merge_prepared" | "explicit_dml"
                    if matches!(observation.reason(), None | Some(RESCAN | RESCAN_ACTIVE))
                        && detail.get("centroid_rebase").is_none() =>
                {
                    rescan.push(elapsed_ms);
                }
                _ => {}
            }
        }
        if algebraic.len() < MIN_SAMPLES || rescan.len() < MIN_SAMPLES {
            return Preference::Unknown;
        }
        algebraic.sort_by(f64::total_cmp);
        rescan.sort_by(f64::total_cmp);
        let algebraic = algebraic[algebraic.len() / 2];
        let rescan = rescan[rescan.len() / 2];
        if rescan < algebraic * 0.8 {
            Preference::Rescan
        } else if algebraic < rescan * 0.8 {
            Preference::Algebraic
        } else {
            Preference::Unknown
        }
    }
}

/// Read a bounded history; an unavailable or malformed sample cannot select a strategy.
pub(crate) fn read_history(pgt_id: i64) -> Option<CostHistory> {
    Spi::connect(|client| {
        let rows = client.select(
            "WITH recent AS MATERIALIZED ( \
               SELECT action, merge_strategy_used, refresh_reason_detail, status, \
                      start_time, end_time, refresh_id \
               FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1 \
               ORDER BY start_time DESC, refresh_id DESC LIMIT $2) \
             SELECT status, action, COALESCE(merge_strategy_used, ''), refresh_reason_detail, \
                    CASE WHEN pg_catalog.isfinite(start_time) AND pg_catalog.isfinite(end_time) \
                         THEN (EXTRACT(EPOCH FROM (end_time - start_time)) * 1000)::float8 \
                         ELSE NULL END \
             FROM recent \
             ORDER BY start_time DESC, refresh_id DESC",
            None,
            &[pgt_id.into(), HISTORY_LIMIT.into()],
        )?;
        let mut observations = Vec::new();
        for row in rows {
            let status = row.get::<String>(1)?.unwrap_or_default();
            let action = row.get::<String>(2)?.unwrap_or_default();
            let strategy = row.get::<String>(3)?.unwrap_or_default();
            let detail = row
                .get::<String>(4)?
                .and_then(|detail| serde_json::from_str(&detail).ok());
            let elapsed_ms = row.get::<f64>(5)?;
            observations.push(Observation {
                completed: status == "COMPLETED" && elapsed_ms.is_some(),
                action,
                strategy,
                detail,
                elapsed_ms,
            });
        }
        Ok::<_, pgrx::spi::Error>(CostHistory { observations })
    })
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(strategy: &str, elapsed_ms: f64, reason: Option<&str>) -> Observation {
        let mut detail = serde_json::json!({
            "schema_version": 1,
            "input_delta_rows": 4,
            "input_delta_rows_may_be_incomplete": false,
            "output_rows_changed": 1,
            "source_rows_estimate": 1000,
            "plan_identity": 42,
            "contract_generation": 7,
        });
        if let Some(reason) = reason {
            detail["centroid_fallback"] = serde_json::json!({"reason": reason});
        }
        Observation {
            completed: true,
            action: "DIFFERENTIAL".into(),
            strategy: strategy.into(),
            detail: Some(detail),
            elapsed_ms: Some(elapsed_ms),
        }
    }

    #[test]
    fn preference_requires_comparable_complete_samples_and_a_margin() {
        let mut observations = Vec::new();
        for _ in 0..3 {
            observations.push(observation("centroid_algebraic", 20.0, None));
            observations.push(observation("merge", 10.0, None));
        }
        let mut history = CostHistory { observations };
        assert_eq!(history.preference(42, 7, 4, 1000), Preference::Rescan);
        assert_eq!(history.preference(43, 7, 4, 1000), Preference::Unknown);
        assert_eq!(history.preference(42, 8, 4, 1000), Preference::Unknown);
        assert_eq!(history.preference(42, 7, 5, 1000), Preference::Unknown);
        assert_eq!(history.preference(42, 7, 4, 1001), Preference::Unknown);
        history.observations[1].detail.as_mut().unwrap()["input_delta_rows_may_be_incomplete"] =
            serde_json::json!(true);
        assert_eq!(history.preference(42, 7, 4, 1000), Preference::Unknown);
        history.observations[1].detail.as_mut().unwrap()["input_delta_rows_may_be_incomplete"] =
            serde_json::json!(false);
        for observation in &mut history.observations {
            observation.elapsed_ms = Some(if observation.strategy == "merge" {
                19.0
            } else {
                20.0
            });
        }
        assert_eq!(history.preference(42, 7, 4, 1000), Preference::Unknown);
        for observation in &mut history.observations {
            observation.elapsed_ms = Some(if observation.strategy == "merge" {
                20.0
            } else {
                10.0
            });
        }
        assert_eq!(history.preference(42, 7, 4, 1000), Preference::Algebraic);
        history.observations[1].detail.as_mut().unwrap()["centroid_fallback"] =
            serde_json::json!({"reason": "SOURCE_AHEAD_OF_FRONTIER"});
        assert_eq!(history.preference(42, 7, 4, 1000), Preference::Unknown);
    }

    #[test]
    fn cooldown_only_matches_the_same_contract() {
        let history = CostHistory {
            observations: vec![observation("merge", 1.0, Some(REBIND))],
        };
        assert!(history.in_cooldown(42, 7));
        assert!(!history.in_cooldown(42, 8));
    }

    #[test]
    fn cooldown_expires_after_eight_data_refreshes() {
        let mut observations = vec![observation("merge", 1.0, Some(RESCAN))];
        for _ in 0..COOLDOWN_REFRESHES {
            observations.insert(0, observation("merge", 1.0, Some(RESCAN_ACTIVE)));
        }
        let history = CostHistory { observations };
        assert!(!history.in_cooldown(42, 7));
    }
}
