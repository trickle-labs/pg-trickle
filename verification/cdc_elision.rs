//! Conservative changed-column decisions. PostgreSQL binary images and SQL
//! lowering are separate conformance boundaries, not axioms about SQL equality.

use vstd::prelude::*;

verus! {
pub fn admitted_image_type(oid: u32) -> (admitted: bool)
    ensures admitted == (oid == 16 || oid == 20 || oid == 21 || oid == 23 || oid == 2950),
{
    oid == 16 || oid == 20 || oid == 21 || oid == 23 || oid == 2950
}

pub fn keep_change(admitted: bool, image_equal: bool) -> (keep: bool)
    ensures keep == (!admitted || !image_equal),
{
    !admitted || !image_equal
}
}

/// Unknown types never invoke a send/equality function and retain their bit.
pub fn changed_column_sql(old: &str, new: &str) -> String {
    let admitted = [16, 20, 21, 23, 2950]
        .into_iter()
        .filter(|oid| admitted_image_type(*oid))
        .map(|oid| oid.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let bit = |keep| if keep { "B'1'" } else { "B'0'" };
    let same = bit(keep_change(true, true));
    let different = bit(keep_change(true, false));
    let unknown = bit(keep_change(false, false));
    format!(
        "(CASE WHEN pg_catalog.pg_typeof({new})::oid IN ({admitted}) \
         THEN CASE WHEN pg_catalog.record_send(ROW({old})) IS DISTINCT FROM \
                        pg_catalog.record_send(ROW({new})) \
                   THEN {different} ELSE {same} END ELSE {unknown} END)::varbit"
    )
}

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_types_and_unequal_images_retain_changes() {
        for admitted in [false, true] {
            for equal in [false, true] {
                assert_eq!(keep_change(admitted, equal), !admitted || !equal);
            }
        }
        assert!(!admitted_image_type(0));
        assert!(!admitted_image_type(u32::MAX));
    }
}
