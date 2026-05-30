//! Per-operator differentiation rules.
//!
//! Each operator has its own differentiation logic that transforms
//! the operator's semantics into a delta computation.
//!
//! ## QW-6: DeltaOperator Trait
//!
//! The `DeltaOperator` trait provides a unified interface for delta generation
//! across all operator types. Implementations can opt in to additional
//! capabilities (immediate-mode support, monotonicity) via default methods.
//!
//! All 22 operator types are registered in the `dispatch` function via
//! `DeltaOperatorDispatch`, enabling future plugin-style extension.

use crate::dvm::diff::{DiffContext, DiffResult};
use crate::dvm::parser::types::OpTree;
use crate::error::PgTrickleError;

/// QW-6 (v0.81.0): Core trait for differential operator implementations.
///
/// Each SQL operator (join, aggregate, filter, etc.) implements this trait
/// to provide its delta-generation logic. The `generate_delta` method
/// receives the operator's fully-resolved children and a mutable `DiffContext`
/// (which carries frontier values, source mappings, and CTE delta state).
///
/// Default implementations of the optional methods return `false`, meaning
/// the operator does not support immediate mode or guarantee monotonicity.
pub trait DeltaOperator {
    /// Compute the delta output for this operator given the deltas of its
    /// children. The `ctx` provides frontier values and shared DVM state;
    /// `children` are the pre-computed delta results for each input.
    fn generate_delta(
        &self,
        ctx: &mut DiffContext,
        op: &OpTree,
    ) -> Result<DiffResult, PgTrickleError>;

    /// Returns `true` if this operator supports IMMEDIATE refresh mode
    /// (i.e., the delta can be computed purely from transition table names
    /// without reading frontier-bounded change buffers).
    fn supports_immediate_mode(&self) -> bool {
        false
    }

    /// Returns `true` if this operator is monotone — i.e., inserting a
    /// row into any source table can only add rows to the output, never
    /// remove them. Monotone operators do not need DELETE handling in
    /// differential refresh.
    fn is_monotone(&self) -> bool {
        false
    }
}

// ── Operator dispatch ─────────────────────────────────────────────────────

/// QW-6 (v0.81.0): Stateless dispatch tokens for each operator type.
///
/// Each unit struct implements `DeltaOperator` by delegating to the
/// existing standalone `diff_*` functions in the operator sub-modules.
/// This keeps backward-compatibility (the existing functions are unchanged)
/// while satisfying the trait contract for new consumers.
pub struct InnerJoinOp;
pub struct LeftOuterJoinOp;
pub struct FullOuterJoinOp;
pub struct SemiJoinOp;
pub struct AntiJoinOp;
pub struct AggregateOp;
pub struct FilterOp;
pub struct ProjectOp;
pub struct ScanOp;
pub struct UnionAllOp;
pub struct IntersectOp;
pub struct ExceptOp;
pub struct DistinctOp;
pub struct WindowOp;
pub struct SubqueryOp;
pub struct ScalarSubqueryOp;
pub struct CteScanOp;
pub struct RecursiveCteOp;
pub struct LateralSubqueryOp;
pub struct LateralFunctionOp;

// Macro to implement DeltaOperator for each dispatch token by calling the
// corresponding standalone diff function from the operator sub-module.
macro_rules! impl_delta_operator {
    ($ty:ty, $func:path) => {
        impl DeltaOperator for $ty {
            fn generate_delta(
                &self,
                ctx: &mut DiffContext,
                op: &OpTree,
            ) -> Result<DiffResult, PgTrickleError> {
                $func(ctx, op)
            }
        }
    };
}

impl_delta_operator!(InnerJoinOp, join::diff_inner_join);
impl_delta_operator!(LeftOuterJoinOp, outer_join::diff_left_join);
impl_delta_operator!(FullOuterJoinOp, full_join::diff_full_join);
impl_delta_operator!(SemiJoinOp, semi_join::diff_semi_join);
impl_delta_operator!(AntiJoinOp, anti_join::diff_anti_join);
impl_delta_operator!(AggregateOp, aggregate::diff_aggregate);
impl_delta_operator!(FilterOp, filter::diff_filter);
impl_delta_operator!(ProjectOp, project::diff_project);
impl_delta_operator!(ScanOp, scan::diff_scan);
impl_delta_operator!(UnionAllOp, union_all::diff_union_all);
impl_delta_operator!(IntersectOp, intersect::diff_intersect);
impl_delta_operator!(ExceptOp, except::diff_except);
impl_delta_operator!(DistinctOp, distinct::diff_distinct);
impl_delta_operator!(WindowOp, window::diff_window);
impl_delta_operator!(SubqueryOp, subquery::diff_subquery);
impl_delta_operator!(ScalarSubqueryOp, scalar_subquery::diff_scalar_subquery);
impl_delta_operator!(CteScanOp, cte_scan::diff_cte_scan);
impl_delta_operator!(RecursiveCteOp, recursive_cte::diff_recursive_cte);
impl_delta_operator!(LateralSubqueryOp, lateral_subquery::diff_lateral_subquery);
impl_delta_operator!(LateralFunctionOp, lateral_function::diff_lateral_function);

pub mod aggregate;
pub mod anti_join;
pub mod cte_scan;
pub mod distinct;
pub mod except;
pub mod filter;
pub mod full_join;
pub mod intersect;
pub mod join;
pub mod join_common;
pub mod lateral_function;
pub mod lateral_subquery;
pub mod outer_join;
pub mod project;
pub mod recursive_cte;
pub mod scalar_subquery;
pub mod scan;
pub mod semi_join;
pub mod subquery;
#[cfg(test)]
pub(crate) mod test_helpers;
pub mod union_all;
pub mod window;
