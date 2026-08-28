//! Structured snapshot planning for DVM operator trees.

use crate::dvm::parser::OpTree;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotUnsupportedReason {
    RecursiveQuery,
    LateralExpansion,
    SetOperation,
    WindowEvaluation,
    MissingCteBody,
}

impl SnapshotUnsupportedReason {
    pub const fn name(&self) -> &'static str {
        match self {
            Self::RecursiveQuery => "recursive_query",
            Self::LateralExpansion => "lateral_expansion",
            Self::SetOperation => "set_operation",
            Self::WindowEvaluation => "window_evaluation",
            Self::MissingCteBody => "missing_cte_body",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPlan {
    ExactPerLeaf,
    ExactCombined,
    PostChangeWithCorrection,
    Unsupported { reason: SnapshotUnsupportedReason },
}

impl SnapshotPlan {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ExactPerLeaf => "exact_per_leaf",
            Self::ExactCombined => "exact_combined",
            Self::PostChangeWithCorrection => "post_change_correction",
            Self::Unsupported { .. } => "unsupported",
        }
    }

    pub fn for_tree(tree: &OpTree) -> Self {
        let facts = Facts::from_tree(tree);
        if let Some(reason) = facts.unsupported {
            return Self::Unsupported { reason };
        }
        if facts.post_change_correction {
            return Self::PostChangeWithCorrection;
        }
        if facts.pure_leaf_tree && facts.scan_count > 1 {
            Self::ExactPerLeaf
        } else {
            Self::ExactCombined
        }
    }

    /// Select the plan used while differentiating a nested join below a
    /// semijoin.  Such joins must stay post-change; reconstructing their old
    /// state interacts with the semijoin's own old-state correction.  Leaf
    /// snapshots remain exact, as they do outside that context.
    pub fn for_tree_in_context(tree: &OpTree, inside_semijoin: bool) -> Self {
        let plan = Self::for_tree(tree);
        if inside_semijoin && matches!(plan, Self::ExactPerLeaf) {
            Self::PostChangeWithCorrection
        } else {
            plan
        }
    }

    pub const fn uses_pre_change(&self) -> bool {
        matches!(self, Self::ExactPerLeaf | Self::ExactCombined)
    }

    pub const fn uses_per_leaf(&self) -> bool {
        matches!(self, Self::ExactPerLeaf)
    }
}

pub fn operator_name(tree: &OpTree) -> &'static str {
    match tree {
        OpTree::Scan { .. } => "Scan",
        OpTree::Filter { .. } => "Filter",
        OpTree::Project { .. } => "Project",
        OpTree::InnerJoin { .. } => "InnerJoin",
        OpTree::LeftJoin { .. } => "LeftJoin",
        OpTree::FullJoin { .. } => "FullJoin",
        OpTree::Aggregate { .. } => "Aggregate",
        OpTree::Distinct { .. } => "Distinct",
        OpTree::UnionAll { .. } => "UnionAll",
        OpTree::Intersect { .. } => "Intersect",
        OpTree::Except { .. } => "Except",
        OpTree::Subquery { .. } => "Subquery",
        OpTree::CteScan { .. } => "CteScan",
        OpTree::RecursiveCte { .. } => "RecursiveCte",
        OpTree::RecursiveSelfRef { .. } => "RecursiveSelfRef",
        OpTree::Window { .. } => "Window",
        OpTree::LateralFunction { .. } => "LateralFunction",
        OpTree::LateralSubquery { .. } => "LateralSubquery",
        OpTree::SemiJoin { .. } => "SemiJoin",
        OpTree::AntiJoin { .. } => "AntiJoin",
        OpTree::ScalarSubquery { .. } => "ScalarSubquery",
        OpTree::ConstantSelect { .. } => "ConstantSelect",
    }
}

#[derive(Default)]
struct Facts {
    scan_count: usize,
    pure_leaf_tree: bool,
    post_change_correction: bool,
    unsupported: Option<SnapshotUnsupportedReason>,
}

impl Facts {
    fn from_tree(tree: &OpTree) -> Self {
        let mut facts = Self {
            pure_leaf_tree: true,
            ..Self::default()
        };
        facts.visit(tree);
        facts
    }

    fn visit(&mut self, tree: &OpTree) {
        if self.unsupported.is_some() {
            return;
        }
        match tree {
            OpTree::Scan { .. } => self.scan_count += 1,
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. } => self.visit(child),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. } => {
                self.visit(left);
                self.visit(right);
            }
            OpTree::SemiJoin { left, right, .. } | OpTree::AntiJoin { left, right, .. } => {
                self.post_change_correction = true;
                self.pure_leaf_tree = false;
                self.visit(left);
                self.visit(right);
            }
            OpTree::Aggregate { child, .. } | OpTree::Distinct { child } => {
                self.pure_leaf_tree = false;
                self.visit(child);
            }
            OpTree::CteScan { body, .. } => {
                self.pure_leaf_tree = false;
                if let Some(body) = body {
                    self.visit(body);
                } else {
                    self.unsupported = Some(SnapshotUnsupportedReason::MissingCteBody);
                }
            }
            OpTree::UnionAll { .. } | OpTree::Intersect { .. } | OpTree::Except { .. } => {
                self.pure_leaf_tree = false;
                self.unsupported = Some(SnapshotUnsupportedReason::SetOperation);
            }
            OpTree::Window { .. } => {
                self.pure_leaf_tree = false;
                self.unsupported = Some(SnapshotUnsupportedReason::WindowEvaluation);
            }
            OpTree::LateralFunction { .. } | OpTree::LateralSubquery { .. } => {
                self.pure_leaf_tree = false;
                self.unsupported = Some(SnapshotUnsupportedReason::LateralExpansion);
            }
            OpTree::RecursiveCte { .. } | OpTree::RecursiveSelfRef { .. } => {
                self.pure_leaf_tree = false;
                self.unsupported = Some(SnapshotUnsupportedReason::RecursiveQuery);
            }
            OpTree::ScalarSubquery {
                subquery, child, ..
            } => {
                self.pure_leaf_tree = false;
                self.visit(subquery);
                self.visit(child);
            }
            OpTree::ConstantSelect { .. } => {
                self.pure_leaf_tree = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    #[test]
    fn plans_simple_join_per_leaf() {
        let tree = inner_join(
            eq_cond("left", "id", "right", "id"),
            scan(1, "left", "public", "left", &["id"]),
            scan(2, "right", "public", "right", &["id"]),
        );
        assert_eq!(SnapshotPlan::for_tree(&tree), SnapshotPlan::ExactPerLeaf);
    }

    #[test]
    fn plans_semijoin_with_post_change_correction() {
        let tree = semi_join(
            eq_cond("left", "id", "right", "id"),
            scan(1, "left", "public", "left", &["id"]),
            scan(2, "right", "public", "right", &["id"]),
        );
        assert_eq!(
            SnapshotPlan::for_tree(&tree),
            SnapshotPlan::PostChangeWithCorrection
        );
    }

    #[test]
    fn plans_aggregate_cte_join_as_exact_combined() {
        let body = aggregate(
            vec![colref("parent_id")],
            vec![count_star("count")],
            scan(2, "child", "public", "c", &["parent_id"]),
        );
        let aggregate_cte = OpTree::CteScan {
            cte_id: 0,
            cte_name: "agg".into(),
            alias: "a".into(),
            columns: vec!["parent_id".into(), "count".into()],
            cte_def_aliases: Vec::new(),
            column_aliases: Vec::new(),
            body: Some(Box::new(body)),
        };
        let tree = inner_join(
            eq_cond("p", "id", "a", "parent_id"),
            scan(1, "parent", "public", "p", &["id"]),
            aggregate_cte,
        );

        assert_eq!(SnapshotPlan::for_tree(&tree), SnapshotPlan::ExactCombined);
    }

    #[test]
    fn reports_unsupported_set_operation() {
        let tree = union_all(vec![
            scan(1, "left", "public", "left", &["id"]),
            scan(2, "right", "public", "right", &["id"]),
        ]);
        assert_eq!(
            SnapshotPlan::for_tree(&tree),
            SnapshotPlan::Unsupported {
                reason: SnapshotUnsupportedReason::SetOperation
            }
        );
    }
}
