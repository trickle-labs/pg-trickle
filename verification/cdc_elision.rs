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

/// Signed contributions used for each complete captured image event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageEvent { Insert, Delete }

/// INSERT adds one image occurrence and DELETE removes one.
pub open spec fn image_event_weight_spec(action: ImageEvent) -> int {
    if action == ImageEvent::Insert { 1 } else { -1 }
}

pub fn image_event_weight(action: ImageEvent) -> (weight: i64)
    ensures (weight as int) == image_event_weight_spec(action),
{
    match action {
        ImageEvent::Insert => 1,
        ImageEvent::Delete => -1,
    }
}

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

/// Keyed history compaction is safe only for one immediate identity and one
/// consumer, with no reset barrier in the selected window.
pub fn can_compact_history(
    has_immediate_unique_identity: bool,
    has_single_consumer: bool,
    has_truncate_barrier: bool,
) -> (eligible: bool)
    ensures eligible == (has_immediate_unique_identity && has_single_consumer && !has_truncate_barrier),
{
    has_immediate_unique_identity && has_single_consumer && !has_truncate_barrier
}

pub open spec fn compaction_eligible_spec(
    has_immediate_unique_identity: bool,
    has_single_consumer: bool,
    has_truncate_barrier: bool,
) -> bool {
    has_immediate_unique_identity && has_single_consumer && !has_truncate_barrier
}

pub open spec fn image_event_in_window_spec(
    event_lsn: u64,
    previous_cut: u64,
    new_cut: u64,
) -> bool {
    event_lsn > previous_cut && event_lsn <= new_cut
}

pub open spec fn window_event_weight_spec(
    action: ImageEvent,
    event_lsn: u64,
    previous_cut: u64,
    new_cut: u64,
) -> int {
    if image_event_in_window_spec(event_lsn, previous_cut, new_cut) {
        image_event_weight_spec(action)
    } else {
        0
    }
}

/// Opposite images can cancel over a combined LSN range while remaining
/// observable in two consumers' separate windows. The production eligibility
/// decision must therefore reject shared-consumer compaction.
pub proof fn separate_consumer_cuts_retain_opposing_images(
    insert_lsn: u64,
    delete_lsn: u64,
    earlier_cut: u64,
    shared_cut: u64,
    later_cut: u64,
)
    requires
        earlier_cut < insert_lsn,
        insert_lsn <= shared_cut,
        shared_cut < delete_lsn,
        delete_lsn <= later_cut,
    ensures
        image_event_weight_spec(ImageEvent::Insert)
            + image_event_weight_spec(ImageEvent::Delete) == 0,
        window_event_weight_spec(
            ImageEvent::Insert, insert_lsn, earlier_cut, shared_cut,
        ) == 1,
        window_event_weight_spec(
            ImageEvent::Delete, delete_lsn, earlier_cut, shared_cut,
        ) == 0,
        window_event_weight_spec(
            ImageEvent::Insert, insert_lsn, shared_cut, later_cut,
        ) == 0,
        window_event_weight_spec(
            ImageEvent::Delete, delete_lsn, shared_cut, later_cut,
        ) == -1,
        !compaction_eligible_spec(true, false, false),
{
    assert(earlier_cut <= shared_cut);
    assert(shared_cut <= later_cut);
    assert(image_event_weight_spec(ImageEvent::Insert) == 1);
    assert(image_event_weight_spec(ImageEvent::Delete) == -1);
}
}

/// Lower the checked per-event signed coefficients for the SQL window sum.
pub fn image_event_weight_sql(action_column: &str) -> String {
    let insert_weight = image_event_weight(ImageEvent::Insert);
    let delete_weight = image_event_weight(ImageEvent::Delete);
    format!("CASE WHEN {action_column} = 'I' THEN {insert_weight} ELSE {delete_weight} END")
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
    let event_weight = image_event_weight_sql("__pgt_action");
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
         FROM (SELECT __images.*, SUM({event_weight}) \
               OVER (PARTITION BY __pgt_row_id,{image_column}) AS {weight_column} \
               FROM (SELECT __raw.*, dense_rank() OVER (PARTITION BY __pgt_row_id \
                     ORDER BY ROW({columns}) USING OPERATOR(pg_catalog.*<)) AS {image_column} \
                     FROM ({delta_sql}) __raw) __images) __weighted \
         WHERE {signs} ORDER BY __pgt_row_id, CASE WHEN __pgt_action='I' THEN 0 ELSE 1 END)",
        signs = signs.join(" OR "),
        event_weight = event_weight,
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

    #[test]
    fn compaction_rejects_missing_identity_shared_consumers_and_truncate_barriers() {
        for has_identity in [false, true] {
            for single_consumer in [false, true] {
                for truncate in [false, true] {
                    assert_eq!(
                        can_compact_history(has_identity, single_consumer, truncate),
                        has_identity && single_consumer && !truncate
                    );
                }
            }
        }
    }

    #[test]
    fn signed_event_weights_match_complete_image_actions() {
        assert_eq!(image_event_weight(ImageEvent::Insert), 1);
        assert_eq!(image_event_weight(ImageEvent::Delete), -1);
        assert_eq!(
            image_event_weight_sql("delta.__pgt_action"),
            "CASE WHEN delta.__pgt_action = 'I' THEN 1 ELSE -1 END"
        );
    }
}
