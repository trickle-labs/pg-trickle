//! The single pg_trickle-owned memory sizing policy.

use pgrx::guc::*;

/// Master budget for pg_trickle-owned in-process accumulations, in MiB.
pub static PGS_MEMORY_BUDGET_MB: GucSetting<i32> = GucSetting::<i32>::new(256);

/// A bounded component of the pg_trickle memory policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryComponent {
    DeltaPipeline,
    TemplatePlanCache,
    DagQueue,
    InvalidationRing,
    ChangeBuffer,
}

impl MemoryComponent {
    pub const ALL: [Self; 5] = [
        Self::DeltaPipeline,
        Self::TemplatePlanCache,
        Self::DagQueue,
        Self::InvalidationRing,
        Self::ChangeBuffer,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::DeltaPipeline => "delta_pipeline",
            Self::TemplatePlanCache => "template_plan_cache",
            Self::DagQueue => "dag_queue",
            Self::InvalidationRing => "invalidation_ring",
            Self::ChangeBuffer => "change_buffer",
        }
    }
}

/// Derived byte limits. The change-buffer value is a guard, not a lossy cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudget {
    pub total_bytes: u64,
    pub delta_pipeline_bytes: u64,
    pub template_plan_cache_bytes: u64,
    pub dag_queue_bytes: u64,
    pub invalidation_ring_bytes: u64,
    pub change_buffer_bytes: u64,
}

impl MemoryBudget {
    pub const MIN_MB: u64 = 16;
    pub const MAX_MB: u64 = 1_048_576;
    const MIB: u64 = 1024 * 1024;

    pub fn from_mb(mb: u64) -> Option<Self> {
        let mb = mb.clamp(Self::MIN_MB, Self::MAX_MB);
        let total_bytes = mb.checked_mul(Self::MIB)?;
        let share = |percent: u64| total_bytes.checked_mul(percent)?.checked_div(100);
        let delta_pipeline_bytes = share(75)?;
        let template_plan_cache_bytes = share(15)?;
        let dag_queue_bytes = share(5)?;
        let invalidation_ring_bytes = total_bytes
            .checked_sub(delta_pipeline_bytes)?
            .checked_sub(template_plan_cache_bytes)?
            .checked_sub(dag_queue_bytes)?;
        Some(Self {
            total_bytes,
            delta_pipeline_bytes,
            template_plan_cache_bytes,
            dag_queue_bytes,
            invalidation_ring_bytes,
            change_buffer_bytes: total_bytes,
        })
    }

    pub fn from_guc() -> Self {
        match Self::from_mb(PGS_MEMORY_BUDGET_MB.get().max(0) as u64) {
            Some(budget) => budget,
            None => Self {
                total_bytes: 256 * Self::MIB,
                delta_pipeline_bytes: 192 * Self::MIB,
                template_plan_cache_bytes: 40 * Self::MIB,
                dag_queue_bytes: 12 * Self::MIB,
                invalidation_ring_bytes: 12 * Self::MIB,
                change_buffer_bytes: 256 * Self::MIB,
            },
        }
    }

    pub const fn limit(self, component: MemoryComponent) -> u64 {
        match component {
            MemoryComponent::DeltaPipeline => self.delta_pipeline_bytes,
            MemoryComponent::TemplatePlanCache => self.template_plan_cache_bytes,
            MemoryComponent::DagQueue => self.dag_queue_bytes,
            MemoryComponent::InvalidationRing => self.invalidation_ring_bytes,
            MemoryComponent::ChangeBuffer => self.change_buffer_bytes,
        }
    }

    /// Existing component settings can only make a derived limit stricter.
    pub const fn stricter_limit(
        self,
        component: MemoryComponent,
        override_bytes: Option<u64>,
    ) -> u64 {
        match override_bytes {
            Some(value) => {
                let limit = self.limit(component);
                if value < limit { value } else { limit }
            }
            None => self.limit(component),
        }
    }
}

/// Register the master memory policy GUC.
pub fn register_memory_gucs() {
    GucRegistry::define_int_guc(
        c"pg_trickle.memory_budget_mb",
        c"Master pg_trickle memory budget in MiB.",
        c"Bounds pg_trickle-owned pipeline, cache, queue, and invalidation accumulations. Change-buffer growth is a lossless storage guard; it never drops committed rows.",
        &PGS_MEMORY_BUDGET_MB,
        MemoryBudget::MIN_MB as i32,
        MemoryBudget::MAX_MB as i32,
        GucContext::Suset,
        GucFlags::default(),
    );
}

/// Return the effective master memory policy.
pub fn pg_trickle_memory_budget() -> MemoryBudget {
    MemoryBudget::from_guc()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_has_all_five_shares() {
        let budget = MemoryBudget::from_mb(256).expect("valid budget");
        assert_eq!(
            budget.delta_pipeline_bytes
                + budget.template_plan_cache_bytes
                + budget.dag_queue_bytes
                + budget.invalidation_ring_bytes,
            budget.total_bytes
        );
        assert_eq!(budget.change_buffer_bytes, budget.total_bytes);
    }

    #[test]
    fn budget_clamps_and_composes_stricter_overrides() {
        let budget = MemoryBudget::from_mb(1).expect("clamped budget");
        assert_eq!(budget.total_bytes, 16 * MemoryBudget::MIB);
        assert_eq!(budget.stricter_limit(MemoryComponent::DagQueue, Some(1)), 1);
        assert_eq!(
            budget.stricter_limit(MemoryComponent::DagQueue, Some(u64::MAX)),
            budget.dag_queue_bytes
        );
    }
}
