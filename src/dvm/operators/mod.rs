//! Per-operator differentiation rules.
//!
//! Each operator has its own differentiation logic that transforms
//! the operator's semantics into a delta computation.

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
pub mod vectorized_agg;
pub mod window;
