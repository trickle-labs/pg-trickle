//! Conservative changed-column and keyed image-action decisions. PostgreSQL
//! images and SQL lowering remain separate conformance boundaries.

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageAction { Insert, Delete, Skip }

/// The signed multiplicity belongs to a complete captured image, not its key.
pub fn image_action(weight: i64) -> (action: ImageAction)
    ensures
        (action == ImageAction::Insert) == (weight > 0),
        (action == ImageAction::Delete) == (weight < 0),
        (action == ImageAction::Skip) == (weight == 0),
{
    if weight > 0 { ImageAction::Insert }
    else if weight < 0 { ImageAction::Delete }
    else { ImageAction::Skip }
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

/// Group by PostgreSQL's complete binary record images, including NULL framing.
pub fn keyed_weight_sql(delta_sql: &str, columns: &str) -> String {
    let vacant = |prefix: &str| {
        let mut name = prefix.to_string();
        let mut suffix = 0usize;
        while columns.contains(&name) {
            name = format!("{prefix}_{suffix}");
            suffix += 1;
        }
        name
    };
    let image_column = vacant("__pgt_image");
    let weight_column = vacant("__pgt_weight");
    let mut signs = Vec::new();
    for (weight, test) in [(1, "> 0"), (-1, "< 0"), (0, "= 0")] {
        let action = match image_action(weight) {
            ImageAction::Insert => "I",
            ImageAction::Delete => "D",
            ImageAction::Skip => continue,
        };
        signs.push(format!(
            "({weight_column} {test} AND __pgt_action = '{action}')"
        ));
    }
    format!(
        "(SELECT DISTINCT ON (__pgt_row_id) __pgt_row_id, __pgt_action, {columns} \
         FROM (SELECT __images.*, SUM(CASE WHEN __pgt_action='I' THEN 1 ELSE -1 END) \
               OVER (PARTITION BY __pgt_row_id,{image_column}) AS {weight_column} \
               FROM (SELECT __raw.*, dense_rank() OVER (PARTITION BY __pgt_row_id \
                     ORDER BY ROW({columns}) USING OPERATOR(pg_catalog.*<)) AS {image_column} \
                     FROM ({delta_sql}) __raw) __images) __weighted \
         WHERE {signs} ORDER BY __pgt_row_id, CASE WHEN __pgt_action='I' THEN 0 ELSE 1 END)",
        signs = signs.join(" OR "),
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
