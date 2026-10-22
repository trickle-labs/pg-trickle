//! Endpoint join terms and conservative affected-pair scope.
//! SQL lowering and complete committed capture remain conformance boundaries.

use vstd::prelude::*;

verus! {
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Boundary { Before, After }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Term {
    pub left_scan: u32,
    pub right_scan: u32,
    pub left_boundary: Boundary,
    pub right_boundary: Boundary,
    pub retract: bool,
}

pub fn admitted_equality_type(oid: u32) -> (admitted: bool)
    ensures admitted == (oid == 16 || oid == 20 || oid == 21 || oid == 23 || oid == 2950),
{
    oid == 16 || oid == 20 || oid == 21 || oid == 23 || oid == 2950
}

pub open spec fn source_weight(
    occurrence: u32, boundary: Boundary, left_scan: u32, right_scan: u32,
    l0: int, l1: int, r0: int, r1: int,
) -> int {
    if occurrence == left_scan {
        if boundary == Boundary::Before { l0 } else { l1 }
    } else if occurrence == right_scan {
        if boundary == Boundary::Before { r0 } else { r1 }
    } else { 0 }
}

pub open spec fn term_weight(
    term: Term, left_scan: u32, right_scan: u32,
    l0: int, l1: int, r0: int, r1: int, predicate_true: bool,
) -> int {
    let value = if predicate_true {
        source_weight(term.left_scan, term.left_boundary, left_scan, right_scan, l0, l1, r0, r1)
        * source_weight(term.right_scan, term.right_boundary, left_scan, right_scan, l0, l1, r0, r1)
    } else { 0 };
    if term.retract { -value } else { value }
}

pub open spec fn endpoint_difference(l0: int, l1: int, r0: int, r1: int, predicate_true: bool) -> int {
    if predicate_true { l1 * r1 - l0 * r0 } else { 0 }
}

/// Accept exactly one new/new insertion and one old/old retraction.
pub fn check_endpoint_plan(left_scan: u32, right_scan: u32, insert: Term, delete: Term) -> (valid: bool)
    ensures
        valid == (left_scan != right_scan
            && insert.left_scan == left_scan && insert.right_scan == right_scan
            && delete.left_scan == left_scan && delete.right_scan == right_scan
            && insert.left_boundary == Boundary::After && insert.right_boundary == Boundary::After
            && delete.left_boundary == Boundary::Before && delete.right_boundary == Boundary::Before
            && !insert.retract && delete.retract),
        valid ==> forall|l0: int, l1: int, r0: int, r1: int, predicate_true: bool|
            term_weight(insert, left_scan, right_scan, l0, l1, r0, r1, predicate_true)
            + term_weight(delete, left_scan, right_scan, l0, l1, r0, r1, predicate_true)
            == endpoint_difference(l0, l1, r0, r1, predicate_true),
{
    left_scan != right_scan
        && insert.left_scan == left_scan && insert.right_scan == right_scan
        && delete.left_scan == left_scan && delete.right_scan == right_scan
        && matches!(insert.left_boundary, Boundary::After) && matches!(insert.right_boundary, Boundary::After)
        && matches!(delete.left_boundary, Boundary::Before) && matches!(delete.right_boundary, Boundary::Before)
        && !insert.retract && delete.retract
}

/// A pair is potentially affected when either complete source identity changed.
pub fn scope_pair(left_changed: bool, right_changed: bool) -> (include: bool)
    ensures include == (left_changed || right_changed),
{
    left_changed || right_changed
}

/// Pointwise bag semantics: an omitted unchanged image-pair has zero delta.
pub proof fn unchanged_pair_has_zero_difference(l0: int, l1: int, r0: int, r1: int, predicate_true: bool)
    requires l0 == l1, r0 == r1,
    ensures endpoint_difference(l0, l1, r0, r1, predicate_true) == 0,
{}
}

/// SQL conformance validates complete identities and binary record comparison.
pub fn endpoint_changes_sql(
    old: &str,
    new: &str,
    columns: &[String],
    insert: Term,
    delete: Term,
) -> Option<String> {
    if !check_endpoint_plan(0, 1, insert, delete) {
        return None;
    }
    let fields = |alias: &str| {
        columns
            .iter()
            .map(|name| format!("{alias}.\"{}\"", name.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut arms = Vec::new();
    for term in [delete, insert] {
        let (source, other) = match term.left_boundary {
            Boundary::Before => (old, new),
            Boundary::After => (new, old),
        };
        let action = if term.retract { "D" } else { "I" };
        arms.push(format!(
            "SELECT a.__pgt_row_id, '{action}'::text AS __pgt_action, {payload} \
             FROM {source} a LEFT JOIN {other} b ON a.__pgt_row_id OPERATOR(pg_catalog.=) b.__pgt_row_id \
             WHERE b.__pgt_row_id IS NULL OR \
               pg_catalog.record_image_eq(ROW({payload}), ROW({other_payload})) IS NOT TRUE",
            payload = fields("a"),
            other_payload = fields("b"),
        ));
    }
    Some(arms.join("\nUNION ALL\n"))
}

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_mixed_snapshots_wrong_signs_and_occurrences() {
        let insert = Term {
            left_scan: 17,
            right_scan: 29,
            left_boundary: Boundary::After,
            right_boundary: Boundary::After,
            retract: false,
        };
        let delete = Term {
            left_boundary: Boundary::Before,
            right_boundary: Boundary::Before,
            retract: true,
            ..insert
        };
        assert!(check_endpoint_plan(17, 29, insert, delete));
        assert!(!check_endpoint_plan(17, 17, insert, delete));
        for bad in [
            Term {
                right_boundary: Boundary::Before,
                ..insert
            },
            Term {
                left_boundary: Boundary::Before,
                ..insert
            },
            Term {
                retract: true,
                ..insert
            },
            Term {
                left_scan: 29,
                right_scan: 17,
                ..insert
            },
        ] {
            assert!(!check_endpoint_plan(17, 29, bad, delete));
        }
        assert!(!check_endpoint_plan(17, 29, insert, insert));
        assert!(!check_endpoint_plan(
            17,
            29,
            insert,
            Term {
                retract: false,
                ..delete
            }
        ));
        for l in [false, true] {
            for r in [false, true] {
                assert_eq!(scope_pair(l, r), l || r);
            }
        }
    }
}
