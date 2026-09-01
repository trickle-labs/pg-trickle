//! Pure v0.90 freshness-controller decisions.
//!
//! This module deliberately has no PostgreSQL dependencies.  Callers load an
//! immutable snapshot and apply the returned proposal in the scheduler.

use std::cmp::Ordering;

use crate::dag::RefreshMode;
use crate::refresh::RefreshAction;

pub const FRESHNESS_SAMPLE_CAP: usize = 128;
pub const FRESHNESS_MIN_SAMPLES: usize = 20;

/// A non-negative, finite duration represented in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FiniteMs(u64);

impl FiniteMs {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn try_from_f64(value: f64) -> Option<Self> {
        (value.is_finite() && value >= 0.0 && value <= u64::MAX as f64)
            .then_some(Self(value as u64))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.0.checked_add(other.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FreshnessDistribution {
    pub samples: usize,
    pub p50_ms: Option<FiniteMs>,
    pub p95_ms: Option<FiniteMs>,
    pub p99_ms: Option<FiniteMs>,
    pub last_ms: Option<FiniteMs>,
}

pub fn bounded_percentile_summary(samples: &[FiniteMs]) -> FreshnessDistribution {
    let mut values = samples.to_vec();
    if values.len() > FRESHNESS_SAMPLE_CAP {
        values = values[values.len() - FRESHNESS_SAMPLE_CAP..].to_vec();
    }
    values.sort_unstable();
    let percentile = |numerator: usize| {
        (!values.is_empty()).then(|| values[((values.len() * numerator).div_ceil(100)).max(1) - 1])
    };
    FreshnessDistribution {
        samples: values.len(),
        p50_ms: percentile(50),
        p95_ms: percentile(95),
        p99_ms: percentile(99),
        last_ms: samples.last().copied(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModeCostEvidence {
    pub mode: RefreshMode,
    pub samples: usize,
    pub min_ms: Option<FiniteMs>,
    pub p50_ms: Option<FiniteMs>,
    pub p95_ms: Option<FiniteMs>,
    pub updated_token: u64,
    pub compatible: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PendingEvidence {
    pub rows: u64,
    pub oldest_commit_age_ms: Option<FiniteMs>,
    pub inflow_rows_per_ms: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoadEvidence {
    pub cpu_percent: Option<f64>,
    pub queue_depth: u64,
    pub lock_waits: u64,
    pub overloaded: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OverrideSet {
    pub interval_ms: Option<FiniteMs>,
    pub mode: Option<RefreshMode>,
    pub batch_size: Option<u64>,
    pub concurrency_ceiling: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SemanticModes {
    pub full: bool,
    pub differential: bool,
}

impl Default for SemanticModes {
    fn default() -> Self {
        Self {
            full: true,
            differential: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CurrentDecision {
    pub interval_ms: Option<FiniteMs>,
    pub mode: Option<RefreshMode>,
    pub batch_size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ControllerInputs {
    pub target_ms: FiniteMs,
    /// Current defining-query/strategy generation, when known.
    pub current_plan_token: Option<u64>,
    pub freshness: FreshnessDistribution,
    pub differential_cost: Option<ModeCostEvidence>,
    pub full_cost: Option<ModeCostEvidence>,
    pub pending: PendingEvidence,
    pub row_width_bytes: Option<u64>,
    pub memory_budget_bytes: Option<u64>,
    pub queue_delay_ms: Option<FiniteMs>,
    pub load: LoadEvidence,
    pub semantic_modes: SemanticModes,
    pub overrides: OverrideSet,
    pub current_decision: CurrentDecision,
    pub min_batch_size: u64,
    pub max_batch_size: u64,
    pub batch_step: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputError {
    InvalidTarget,
    InvalidSemanticModes,
    InvalidFreshness,
    InvalidCpu,
    InvalidInflow,
    InvalidCost,
    InvalidBatchBounds,
    InvalidOverride,
}

impl ControllerInputs {
    /// Validate values that cannot be represented safely by the controller.
    pub fn validate(&self) -> Result<(), InputError> {
        if self.target_ms == FiniteMs::ZERO {
            return Err(InputError::InvalidTarget);
        }
        if !self.semantic_modes.full && !self.semantic_modes.differential {
            return Err(InputError::InvalidSemanticModes);
        }
        let freshness = self.freshness;
        if freshness.samples > FRESHNESS_SAMPLE_CAP
            || (freshness.samples == 0
                && (freshness.p50_ms.is_some()
                    || freshness.p95_ms.is_some()
                    || freshness.p99_ms.is_some()
                    || freshness.last_ms.is_some()))
            || (freshness.samples > 0
                && (freshness.p50_ms.is_none()
                    || freshness.p95_ms.is_none()
                    || freshness.p99_ms.is_none()
                    || freshness.last_ms.is_none()))
            || freshness.p50_ms > freshness.p95_ms
            || freshness.p95_ms > freshness.p99_ms
        {
            return Err(InputError::InvalidFreshness);
        }
        if self
            .load
            .cpu_percent
            .is_some_and(|cpu| !cpu.is_finite() || cpu < 0.0 || cpu > 100.0)
        {
            return Err(InputError::InvalidCpu);
        }
        if self
            .pending
            .inflow_rows_per_ms
            .is_some_and(|rate| !rate.is_finite() || rate < 0.0)
        {
            return Err(InputError::InvalidInflow);
        }
        for (mode, evidence) in [
            (RefreshMode::Differential, self.differential_cost),
            (RefreshMode::Full, self.full_cost),
        ]
        .into_iter()
        .filter_map(|(mode, evidence)| evidence.map(|evidence| (mode, evidence)))
        {
            if evidence.mode != mode
                || !evidence.compatible
                || evidence.samples == 0
                || evidence.min_ms.is_none()
                || evidence.p50_ms.is_none()
                || evidence.p95_ms.is_none()
                || evidence.min_ms > evidence.p50_ms
                || evidence.p50_ms > evidence.p95_ms
                || self.current_plan_token != Some(evidence.updated_token)
            {
                return Err(InputError::InvalidCost);
            }
        }
        if self.min_batch_size == 0
            || self.max_batch_size < self.min_batch_size
            || self.batch_step == 0
        {
            return Err(InputError::InvalidBatchBounds);
        }
        if self.overrides.interval_ms == Some(FiniteMs::ZERO)
            || self.overrides.batch_size == Some(0)
            || self.overrides.concurrency_ceiling == Some(0)
            || self.overrides.mode == Some(RefreshMode::Immediate)
        {
            return Err(InputError::InvalidOverride);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlaStatus {
    Meeting,
    AtRisk,
    Breaching,
    Infeasible,
    InsufficientData,
    EvidenceUnavailable,
    NotApplicable,
}

impl SlaStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Meeting => "MEETING",
            Self::AtRisk => "AT_RISK",
            Self::Breaching => "BREACHING",
            Self::Infeasible => "INFEASIBLE",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::EvidenceUnavailable => "EVIDENCE_UNAVAILABLE",
            Self::NotApplicable => "NOT_APPLICABLE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PriorityClass {
    Overdue,
    AtRisk,
    Meeting,
    Untargeted,
    Infeasible,
}

impl PriorityClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Overdue => "OVERDUE",
            Self::AtRisk => "AT_RISK",
            Self::Meeting => "MEETING",
            Self::Untargeted => "UNTARGETED",
            Self::Infeasible => "INFEASIBLE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasonCode {
    NoPendingChanges,
    Due,
    WaitingForSlack,
    EvidenceUnavailable,
    BootstrapCostEvidence,
    Override,
    MeasuredCost,
    MemoryCap,
    Overloaded,
}

impl ReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoPendingChanges => "NO_PENDING_CHANGES",
            Self::Due => "DUE",
            Self::WaitingForSlack => "WAITING_FOR_SLACK",
            Self::EvidenceUnavailable => "EVIDENCE_UNAVAILABLE",
            Self::BootstrapCostEvidence => "BOOTSTRAP_COST_EVIDENCE",
            Self::Override => "OVERRIDE",
            Self::MeasuredCost => "MEASURED_COST",
            Self::MemoryCap => "MEMORY_CAP",
            Self::Overloaded => "OVERLOADED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerDecision {
    pub due: bool,
    pub next_due_in_ms: Option<FiniteMs>,
    pub action: RefreshAction,
    pub batch_size: Option<u64>,
    pub deadline_slack_ms: Option<i64>,
    pub priority_class: PriorityClass,
    pub defer: bool,
    pub feasibility: SlaStatus,
    pub worker_demand: u32,
    pub reason_code: ReasonCode,
}

impl ControllerDecision {
    /// Render the stable, bounded advisory result stored beside the inputs.
    pub fn as_json(self) -> serde_json::Value {
        serde_json::json!({
            "due": self.due,
            "next_due_in_ms": self.next_due_in_ms.map(FiniteMs::get),
            "action": self.action.as_str(),
            "batch_size": self.batch_size,
            "deadline_slack_ms": self.deadline_slack_ms,
            "priority_class": self.priority_class.as_str(),
            "defer": self.defer,
            "feasibility": self.feasibility.as_str(),
            "worker_demand": self.worker_demand,
            "reason_code": self.reason_code.as_str(),
        })
    }
}

fn mode_cost(input: &ControllerInputs, mode: RefreshMode) -> Option<FiniteMs> {
    let evidence = match mode {
        RefreshMode::Full => input.full_cost,
        RefreshMode::Differential => input.differential_cost,
        RefreshMode::Immediate => None,
    }?;
    (evidence.mode == mode
        && evidence.compatible
        && evidence.samples > 0
        && input.current_plan_token == Some(evidence.updated_token))
    .then_some(evidence.p95_ms?)
}

fn valid_current_mode(input: &ControllerInputs) -> Option<RefreshMode> {
    match input.current_decision.mode {
        Some(RefreshMode::Full) if input.semantic_modes.full => Some(RefreshMode::Full),
        Some(RefreshMode::Differential) if input.semantic_modes.differential => {
            Some(RefreshMode::Differential)
        }
        _ => None,
    }
}

fn choose_mode(input: &ControllerInputs) -> (RefreshMode, ReasonCode) {
    if let Some(mode) = input.overrides.mode
        && ((mode == RefreshMode::Full && input.semantic_modes.full)
            || (mode == RefreshMode::Differential && input.semantic_modes.differential))
    {
        return (mode, ReasonCode::Override);
    }
    let differential = input
        .semantic_modes
        .differential
        .then(|| mode_cost(input, RefreshMode::Differential));
    let full = input
        .semantic_modes
        .full
        .then(|| mode_cost(input, RefreshMode::Full));
    match (differential.flatten(), full.flatten()) {
        (Some(diff), Some(full)) if diff <= full => {
            (RefreshMode::Differential, ReasonCode::MeasuredCost)
        }
        (Some(_), Some(_)) => (RefreshMode::Differential, ReasonCode::MeasuredCost),
        (Some(_), None) => (
            valid_current_mode(input).unwrap_or(RefreshMode::Differential),
            if valid_current_mode(input).is_some() {
                ReasonCode::BootstrapCostEvidence
            } else {
                ReasonCode::MeasuredCost
            },
        ),
        (None, Some(_)) => (
            valid_current_mode(input).unwrap_or(RefreshMode::Full),
            if valid_current_mode(input).is_some() {
                ReasonCode::BootstrapCostEvidence
            } else {
                ReasonCode::MeasuredCost
            },
        ),
        (None, None) => (
            valid_current_mode(input).unwrap_or({
                if input.semantic_modes.differential {
                    RefreshMode::Differential
                } else {
                    RefreshMode::Full
                }
            }),
            ReasonCode::BootstrapCostEvidence,
        ),
    }
}

fn mode_min_cost(input: &ControllerInputs, mode: RefreshMode) -> Option<FiniteMs> {
    let evidence = match mode {
        RefreshMode::Full => input.full_cost,
        RefreshMode::Differential => input.differential_cost,
        RefreshMode::Immediate => None,
    }?;
    (evidence.mode == mode
        && evidence.compatible
        && evidence.samples >= 5
        && input.current_plan_token == Some(evidence.updated_token))
    .then_some(evidence.min_ms?)
}

fn minimum_eligible_cost(input: &ControllerInputs) -> Option<FiniteMs> {
    if let Some(mode) = input.overrides.mode {
        return mode_min_cost(input, mode);
    }
    [
        input
            .semantic_modes
            .differential
            .then(|| mode_min_cost(input, RefreshMode::Differential)),
        input
            .semantic_modes
            .full
            .then(|| mode_min_cost(input, RefreshMode::Full)),
    ]
    .into_iter()
    .flatten()
    .flatten()
    .min()
}

/// Whether the measured-cost proof is strong enough to count one
/// infeasibility observation.  The three-observation transition belongs to
/// [`SlaHysteresis`].
pub fn infeasibility_observation(input: &ControllerInputs) -> bool {
    input.current_plan_token.is_some()
        && minimum_eligible_cost(input).is_some_and(|cost| cost > input.target_ms)
}

/// Propose all per-table controller outputs using checked/saturating arithmetic.
pub fn propose(input: &ControllerInputs) -> ControllerDecision {
    let pending = input.pending.rows > 0;
    let input_invalid = input.validate().is_err();
    let (mode, mode_reason) = choose_mode(input);
    // Missing elapsed-cost evidence must not make a pending table look safe.
    let cost = mode_cost(input, mode).unwrap_or(input.target_ms);
    let age = input.pending.oldest_commit_age_ms.unwrap_or(FiniteMs::ZERO);
    let queue = input.queue_delay_ms.unwrap_or(FiniteMs::ZERO);
    let completion = age.saturating_add(queue).saturating_add(cost);
    let due_target = input.overrides.interval_ms.unwrap_or(input.target_ms);
    let slack = i64::try_from(due_target.get()).unwrap_or(i64::MAX)
        - i64::try_from(completion.get()).unwrap_or(i64::MAX);
    let age_unknown = pending && input.pending.oldest_commit_age_ms.is_none();
    let due = pending && (age_unknown || slack <= 0);
    let status = if input_invalid {
        SlaStatus::EvidenceUnavailable
    } else if input.freshness.samples < FRESHNESS_MIN_SAMPLES {
        SlaStatus::InsufficientData
    } else if input.freshness.p95_ms.is_none() {
        SlaStatus::EvidenceUnavailable
    } else if slack <= 0
        || cost.get().saturating_mul(10) >= input.target_ms.get().saturating_mul(8)
        || input
            .freshness
            .p95_ms
            .is_some_and(|p95| p95 > input.target_ms)
    {
        SlaStatus::AtRisk
    } else {
        SlaStatus::Meeting
    };
    let defer = pending && !age_unknown && input.load.overloaded && slack > 0;
    let effective_batch_size = batch_size(input);
    let batch_memory_capped = input.overrides.batch_size.is_some_and(|requested| {
        effective_batch_size.is_some_and(|effective| effective < requested)
    });
    ControllerDecision {
        due,
        next_due_in_ms: (!due && pending).then(|| FiniteMs::new(slack.max(0) as u64)),
        action: if pending {
            match mode {
                RefreshMode::Full => RefreshAction::Full,
                RefreshMode::Differential => RefreshAction::Differential,
                RefreshMode::Immediate => RefreshAction::Full,
            }
        } else {
            RefreshAction::NoData
        },
        batch_size: effective_batch_size,
        deadline_slack_ms: pending.then_some(slack),
        priority_class: if status == SlaStatus::Infeasible {
            PriorityClass::Infeasible
        } else if due {
            PriorityClass::Overdue
        } else if matches!(status, SlaStatus::AtRisk | SlaStatus::Breaching) {
            PriorityClass::AtRisk
        } else {
            PriorityClass::Meeting
        },
        defer,
        feasibility: status,
        worker_demand: if due && !defer && status != SlaStatus::Infeasible {
            1
        } else {
            0
        },
        reason_code: if !pending {
            ReasonCode::NoPendingChanges
        } else if !input.pending.oldest_commit_age_ms.is_some() {
            ReasonCode::EvidenceUnavailable
        } else if defer {
            ReasonCode::Overloaded
        } else if due {
            ReasonCode::Due
        } else if batch_memory_capped {
            ReasonCode::MemoryCap
        } else if slack > 0 {
            ReasonCode::WaitingForSlack
        } else if input.overrides.mode.is_some() || input.overrides.batch_size.is_some() {
            ReasonCode::Override
        } else {
            mode_reason
        },
    }
}

fn batch_size(input: &ControllerInputs) -> Option<u64> {
    let min = input.min_batch_size.max(1);
    let max = input.max_batch_size.max(min);
    let memory_max = match (input.row_width_bytes, input.memory_budget_bytes) {
        (Some(width), Some(budget)) if width > 0 && input.batch_step > 0 => {
            let raw = budget / width;
            (raw / input.batch_step)
                .saturating_mul(input.batch_step)
                .clamp(min, max)
        }
        _ => max,
    };
    if let Some(requested) = input.overrides.batch_size {
        return Some(requested.min(max).max(min).min(memory_max));
    }
    Some(memory_max)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Increase,
    Decrease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Hysteresis {
    direction: Option<Direction>,
    count: u8,
    evidence_token: Option<u64>,
}

impl Hysteresis {
    pub fn observe(&mut self, direction: Option<Direction>) -> bool {
        match direction {
            Some(direction) if self.direction == Some(direction) => {
                self.count = self.count.saturating_add(1)
            }
            Some(direction) => {
                self.direction = Some(direction);
                self.count = 1;
            }
            None => {
                self.reset();
                return false;
            }
        }
        if self.count >= 3 {
            self.reset();
            true
        } else {
            false
        }
    }

    /// Apply a direction only once for each new evidence token.
    pub fn observe_sample(&mut self, direction: Option<Direction>, evidence_token: u64) -> bool {
        if self.evidence_token == Some(evidence_token) {
            return false;
        }
        self.evidence_token = Some(evidence_token);
        self.observe(direction)
    }

    pub const fn reset(&mut self) {
        self.direction = None;
        self.count = 0;
        self.evidence_token = None;
    }
    pub const fn count(self) -> u8 {
        self.count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ModeHysteresis {
    candidate: Option<RefreshMode>,
    count: u8,
    plan_token: Option<u64>,
    evidence_token: Option<u64>,
}

impl ModeHysteresis {
    pub fn observe(&mut self, mode: RefreshMode, plan_token: u64) -> bool {
        self.observe_inner(mode, plan_token, None)
    }

    /// Apply a mode observation only once for each compatible evidence token.
    pub fn observe_sample(
        &mut self,
        mode: RefreshMode,
        plan_token: u64,
        evidence_token: u64,
    ) -> bool {
        self.observe_inner(mode, plan_token, Some(evidence_token))
    }

    fn observe_inner(
        &mut self,
        mode: RefreshMode,
        plan_token: u64,
        evidence_token: Option<u64>,
    ) -> bool {
        if self.plan_token != Some(plan_token) {
            self.plan_token = Some(plan_token);
            self.candidate = None;
            self.count = 0;
            self.evidence_token = None;
        }
        if evidence_token.is_some() && self.evidence_token == evidence_token {
            return false;
        }
        self.evidence_token = evidence_token;
        if self.candidate == Some(mode) {
            self.count = self.count.saturating_add(1);
        } else {
            self.candidate = Some(mode);
            self.count = 1;
        }
        if self.count >= 3 {
            self.candidate = None;
            self.count = 0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SlaHysteresis {
    pub breach_streak: u8,
    pub recovery_streak: u8,
    pub infeasible_streak: u8,
    pub infeasible_recovery_streak: u8,
    last_breach_token: Option<u64>,
    last_infeasible_token: Option<u64>,
}

impl SlaHysteresis {
    pub fn observe_breach(&mut self, exceeds: bool) -> bool {
        if exceeds {
            self.breach_streak = self.breach_streak.saturating_add(1);
            self.recovery_streak = 0;
        } else {
            self.recovery_streak = self.recovery_streak.saturating_add(1);
            self.breach_streak = 0;
        }
        self.breach_streak >= 3
    }

    /// Count a breach/recovery evaluation once for each newly settled sample.
    pub fn observe_breach_sample(&mut self, sample_token: u64, exceeds: bool) -> bool {
        if self.last_breach_token == Some(sample_token) {
            return false;
        }
        self.last_breach_token = Some(sample_token);
        self.observe_breach(exceeds)
    }

    pub fn observe_infeasible(
        &mut self,
        minimum_cost: Option<FiniteMs>,
        target: FiniteMs,
        samples: usize,
    ) -> bool {
        if samples >= 5 && minimum_cost.is_some_and(|cost| cost > target) {
            self.infeasible_streak = self.infeasible_streak.saturating_add(1);
            self.infeasible_recovery_streak = 0;
        } else {
            self.infeasible_recovery_streak = self.infeasible_recovery_streak.saturating_add(1);
            self.infeasible_streak = 0;
        }
        self.infeasible_streak >= 3
    }

    pub const fn is_breaching(self) -> bool {
        self.breach_streak >= 3
    }

    pub const fn is_infeasible(self) -> bool {
        self.infeasible_streak >= 3
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Count an infeasibility evaluation once for each new compatible sample.
    pub fn observe_infeasible_sample(
        &mut self,
        sample_token: u64,
        minimum_cost: Option<FiniteMs>,
        target: FiniteMs,
        samples: usize,
    ) -> bool {
        if self.last_infeasible_token == Some(sample_token) {
            return false;
        }
        self.last_infeasible_token = Some(sample_token);
        self.observe_infeasible(minimum_cost, target, samples)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerDemandInputs {
    pub cpu_percent: Option<u8>,
    pub at_risk_feasible: u32,
    pub queue_depth: u32,
    pub current: u32,
    pub min: u32,
    pub max: u32,
    pub active: u32,
    pub all_idle: bool,
    pub enabled: bool,
}

pub fn resize_worker_target(input: WorkerDemandInputs, state: &mut Hysteresis) -> u32 {
    // Treat reversed bounds conservatively instead of allowing `clamp` to
    // panic on malformed shared-memory/configuration input.
    let min = input.min.min(input.max);
    let max = input.min.max(input.max);
    let current = input.current.clamp(min, max);
    let (direction, limit) = if !input.enabled {
        return current;
    } else if input.cpu_percent.is_some_and(|cpu| cpu > 70) {
        (
            Some(Direction::Decrease),
            current.saturating_sub(1).max(min),
        )
    } else if input.at_risk_feasible > 0 || input.queue_depth > current.saturating_mul(2) {
        (
            Some(Direction::Increase),
            current.saturating_add(1).min(max),
        )
    } else if input.all_idle && input.active == 0 {
        (
            Some(Direction::Decrease),
            current.saturating_sub(1).max(min),
        )
    } else {
        (None, current)
    };
    if state.observe(direction) {
        limit
    } else {
        current
    }
}

pub fn priority_order(a: &ControllerDecision, b: &ControllerDecision) -> Ordering {
    a.priority_class.cmp(&b.priority_class).then_with(|| {
        a.deadline_slack_ms
            .unwrap_or(i64::MAX)
            .cmp(&b.deadline_slack_ms.unwrap_or(i64::MAX))
    })
}

/// Stable public name used by diagnostics and release tooling.
pub type ControllerHysteresis = Hysteresis;

#[cfg(test)]
mod tests {
    use super::*;

    fn controller_inputs() -> ControllerInputs {
        ControllerInputs {
            target_ms: FiniteMs::new(100),
            current_plan_token: Some(1),
            freshness: bounded_percentile_summary(&[FiniteMs::new(1); 20]),
            differential_cost: None,
            full_cost: None,
            pending: PendingEvidence {
                rows: 0,
                oldest_commit_age_ms: None,
                inflow_rows_per_ms: None,
            },
            row_width_bytes: Some(10),
            memory_budget_bytes: Some(100),
            queue_delay_ms: Some(FiniteMs::ZERO),
            load: LoadEvidence {
                cpu_percent: Some(10.0),
                queue_depth: 0,
                lock_waits: 0,
                overloaded: false,
            },
            semantic_modes: SemanticModes::default(),
            overrides: OverrideSet::default(),
            current_decision: CurrentDecision::default(),
            min_batch_size: 1,
            max_batch_size: 100,
            batch_step: 10,
        }
    }

    fn cost(mode: RefreshMode, p95_ms: u64, samples: usize) -> ModeCostEvidence {
        ModeCostEvidence {
            mode,
            samples,
            min_ms: Some(FiniteMs::new(p95_ms)),
            p50_ms: Some(FiniteMs::new(p95_ms)),
            p95_ms: Some(FiniteMs::new(p95_ms)),
            updated_token: 1,
            compatible: true,
        }
    }

    #[test]
    fn percentile_is_bounded_and_nearest_ranked() {
        let values: Vec<_> = (0..200).map(FiniteMs::new).collect();
        let summary = bounded_percentile_summary(&values);
        assert_eq!(summary.samples, 128);
        assert_eq!(summary.p95_ms, Some(FiniteMs::new(193)));
        assert_eq!(summary.last_ms, Some(FiniteMs::new(199)));
    }

    #[test]
    fn finite_inputs_reject_bad_floats() {
        assert!(FiniteMs::try_from_f64(-1.0).is_none());
        assert!(FiniteMs::try_from_f64(f64::NAN).is_none());
        assert!(FiniteMs::try_from_f64(f64::INFINITY).is_none());
        assert_eq!(FiniteMs::try_from_f64(12.9), Some(FiniteMs::new(12)));
    }

    #[test]
    fn validation_rejects_missing_or_stale_cost_identity() {
        let mut input = controller_inputs();
        input.target_ms = FiniteMs::ZERO;
        assert_eq!(input.validate(), Err(InputError::InvalidTarget));

        input.target_ms = FiniteMs::new(100);
        input.full_cost = Some(cost(RefreshMode::Full, 10, 5));
        input.current_plan_token = None;
        assert_eq!(input.validate(), Err(InputError::InvalidCost));

        input.current_plan_token = Some(2);
        assert_eq!(input.validate(), Err(InputError::InvalidCost));

        input.current_plan_token = Some(1);
        input.freshness = FreshnessDistribution {
            samples: 1,
            p50_ms: None,
            p95_ms: Some(FiniteMs::new(1)),
            p99_ms: None,
            last_ms: None,
        };
        assert_eq!(input.validate(), Err(InputError::InvalidFreshness));
    }

    #[test]
    fn unknown_age_dispatches_even_when_overloaded() {
        let mut input = controller_inputs();
        input.pending.rows = 1;
        input.load.overloaded = true;
        let decision = propose(&input);
        assert!(decision.due);
        assert!(!decision.defer);
        assert_eq!(decision.reason_code, ReasonCode::EvidenceUnavailable);
    }

    #[test]
    fn one_sided_evidence_does_not_switch_a_valid_current_mode() {
        let mut input = controller_inputs();
        input.pending.rows = 1;
        input.pending.oldest_commit_age_ms = Some(FiniteMs::ZERO);
        input.current_decision.mode = Some(RefreshMode::Full);
        input.differential_cost = Some(cost(RefreshMode::Differential, 10, 5));
        let decision = propose(&input);
        assert_eq!(decision.action, RefreshAction::Full);
    }

    #[test]
    fn measured_cost_selects_the_lower_eligible_mode() {
        let mut input = controller_inputs();
        input.pending.rows = 1;
        input.pending.oldest_commit_age_ms = Some(FiniteMs::ZERO);
        input.differential_cost = Some(cost(RefreshMode::Differential, 10, 5));
        input.full_cost = Some(cost(RefreshMode::Full, 20, 5));
        assert_eq!(propose(&input).action, RefreshAction::Differential);
    }

    #[test]
    fn explicit_batch_is_capped_by_memory_budget() {
        let mut input = controller_inputs();
        input.overrides.batch_size = Some(100);
        assert_eq!(propose(&input).batch_size, Some(10));
    }

    #[test]
    fn freshness_risk_is_not_immediate_infeasibility() {
        let mut input = controller_inputs();
        input.pending.rows = 1;
        input.pending.oldest_commit_age_ms = Some(FiniteMs::ZERO);
        input.freshness = bounded_percentile_summary(&[FiniteMs::new(150); 20]);
        input.full_cost = Some(cost(RefreshMode::Full, 200, 5));
        let decision = propose(&input);
        assert_eq!(decision.feasibility, SlaStatus::AtRisk);
        assert!(infeasibility_observation(&input));
    }

    #[test]
    fn infeasibility_uses_fastest_observed_cost_not_p95() {
        let mut input = controller_inputs();
        input.full_cost = Some(ModeCostEvidence {
            mode: RefreshMode::Full,
            samples: 5,
            min_ms: Some(FiniteMs::new(50)),
            p50_ms: Some(FiniteMs::new(100)),
            p95_ms: Some(FiniteMs::new(200)),
            updated_token: 1,
            compatible: true,
        });
        assert!(!infeasibility_observation(&input));
    }

    #[test]
    fn overload_defers_only_pending_work_with_positive_slack() {
        let mut input = controller_inputs();
        input.pending.rows = 1;
        input.pending.oldest_commit_age_ms = Some(FiniteMs::ZERO);
        input.full_cost = Some(cost(RefreshMode::Full, 10, 5));
        input.load.overloaded = true;
        let decision = propose(&input);
        assert!(!decision.due);
        assert!(decision.defer);
        assert_eq!(decision.reason_code, ReasonCode::Overloaded);
        assert_eq!(decision.next_due_in_ms, Some(FiniteMs::new(90)));
    }

    #[test]
    fn due_and_override_precedence_are_deterministic() {
        let input = ControllerInputs {
            target_ms: FiniteMs::new(100),
            current_plan_token: None,
            freshness: bounded_percentile_summary(&[FiniteMs::new(1); 20]),
            differential_cost: None,
            full_cost: None,
            pending: PendingEvidence {
                rows: 1,
                oldest_commit_age_ms: Some(FiniteMs::new(100)),
                inflow_rows_per_ms: None,
            },
            row_width_bytes: Some(10),
            memory_budget_bytes: Some(100),
            queue_delay_ms: Some(FiniteMs::ZERO),
            load: LoadEvidence {
                cpu_percent: Some(10.0),
                queue_depth: 0,
                lock_waits: 0,
                overloaded: false,
            },
            semantic_modes: SemanticModes::default(),
            overrides: OverrideSet {
                mode: Some(RefreshMode::Full),
                ..Default::default()
            },
            current_decision: CurrentDecision::default(),
            min_batch_size: 1,
            max_batch_size: 100,
            batch_step: 1,
        };
        let decision = propose(&input);
        assert!(decision.due);
        assert_eq!(decision.action, RefreshAction::Full);
        assert_eq!(decision.batch_size, Some(10));
    }

    #[test]
    fn hysteresis_requires_three_observations_and_plan_reset() {
        let mut mode = ModeHysteresis::default();
        assert!(!mode.observe(RefreshMode::Full, 1));
        assert!(!mode.observe(RefreshMode::Full, 1));
        assert!(mode.observe(RefreshMode::Full, 1));
        assert!(!mode.observe(RefreshMode::Full, 2));
    }

    #[test]
    fn hysteresis_ignores_repeated_evidence_tokens() {
        let mut direction = Hysteresis::default();
        assert!(!direction.observe_sample(Some(Direction::Increase), 1));
        assert!(!direction.observe_sample(Some(Direction::Increase), 1));
        assert!(!direction.observe_sample(Some(Direction::Increase), 2));
        assert!(direction.observe_sample(Some(Direction::Increase), 3));

        let mut mode = ModeHysteresis::default();
        assert!(!mode.observe_sample(RefreshMode::Full, 1, 10));
        assert!(!mode.observe_sample(RefreshMode::Full, 1, 10));
        assert!(!mode.observe_sample(RefreshMode::Full, 1, 11));
        assert!(mode.observe_sample(RefreshMode::Full, 1, 12));
    }

    #[test]
    fn sla_hysteresis_requires_three_new_samples() {
        let mut hysteresis = SlaHysteresis::default();
        assert!(!hysteresis.observe_breach_sample(1, true));
        assert!(!hysteresis.observe_breach_sample(1, true));
        assert!(!hysteresis.observe_breach_sample(2, true));
        assert!(hysteresis.observe_breach_sample(3, true));
        assert!(hysteresis.is_breaching());
        assert!(!hysteresis.observe_breach_sample(3, false));
        assert_eq!(hysteresis.recovery_streak, 0);

        assert!(!hysteresis.observe_infeasible_sample(
            1,
            Some(FiniteMs::new(101)),
            FiniteMs::new(100),
            5
        ));
        assert!(!hysteresis.observe_infeasible_sample(
            2,
            Some(FiniteMs::new(101)),
            FiniteMs::new(100),
            5
        ));
        assert!(hysteresis.observe_infeasible_sample(
            3,
            Some(FiniteMs::new(101)),
            FiniteMs::new(100),
            5
        ));
        assert!(hysteresis.is_infeasible());
        assert!(!hysteresis.observe_infeasible_sample(
            4,
            Some(FiniteMs::new(99)),
            FiniteMs::new(100),
            5
        ));
        assert!(!hysteresis.observe_infeasible_sample(
            5,
            Some(FiniteMs::new(99)),
            FiniteMs::new(100),
            5
        ));
        assert!(!hysteresis.observe_infeasible_sample(
            6,
            Some(FiniteMs::new(99)),
            FiniteMs::new(100),
            5
        ));
        assert_eq!(hysteresis.infeasible_recovery_streak, 3);
        assert!(!hysteresis.is_infeasible());
    }

    #[test]
    fn cpu_wins_worker_growth() {
        let mut state = Hysteresis::default();
        let input = WorkerDemandInputs {
            cpu_percent: Some(90),
            at_risk_feasible: 2,
            queue_depth: 20,
            current: 3,
            min: 1,
            max: 8,
            active: 3,
            all_idle: false,
            enabled: true,
        };
        assert_eq!(resize_worker_target(input, &mut state), 3);
        assert_eq!(resize_worker_target(input, &mut state), 3);
        assert_eq!(resize_worker_target(input, &mut state), 2);
    }

    #[test]
    fn invalid_worker_bounds_do_not_panic() {
        let input = WorkerDemandInputs {
            cpu_percent: None,
            at_risk_feasible: 0,
            queue_depth: 0,
            current: 4,
            min: 8,
            max: 1,
            active: 0,
            all_idle: false,
            enabled: false,
        };
        assert_eq!(resize_worker_target(input, &mut Hysteresis::default()), 4);
    }

    #[test]
    fn priority_places_missing_slack_after_known_slack() {
        let mut known = propose(&controller_inputs());
        known.deadline_slack_ms = Some(10);
        let mut unknown = known;
        unknown.deadline_slack_ms = None;
        assert_eq!(priority_order(&known, &unknown), Ordering::Less);
    }
}
