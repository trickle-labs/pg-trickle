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
pub open spec fn can_compact_history_spec(
    has_immediate_unique_identity: bool,
    has_single_consumer: bool,
    has_truncate_barrier: bool,
) -> bool {
    has_immediate_unique_identity && (has_single_consumer && !has_truncate_barrier)
}

pub fn can_compact_history(
    has_immediate_unique_identity: bool,
    has_single_consumer: bool,
    has_truncate_barrier: bool,
) -> (eligible: bool)
    ensures eligible == can_compact_history_spec(
        has_immediate_unique_identity, has_single_consumer, has_truncate_barrier,
    ),
{
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

/// One committed change-buffer image, keyed by the complete logical identity
/// and complete row representation. `source_incarnation` prevents reuse of a
/// key from joining histories across a recreated source; all rows from one
/// transaction share its commit LSN.
#[allow(dead_code)]
pub struct CapturedImageEvent {
    pub row_identity: Seq<u8>,
    pub row_image: Seq<u8>,
    pub source_incarnation: u64,
    pub transaction_id: u64,
    pub commit_lsn: u64,
    pub action: ImageEvent,
}

pub open spec fn captured_event_in_window(
    event: CapturedImageEvent,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
) -> bool {
    event.source_incarnation == source_incarnation
        && image_event_in_window_spec(event.commit_lsn, previous_cut, new_cut)
}

/// Signed multiplicity for one complete row-image bucket in one source
/// incarnation and one consumer's `(previous_cut, new_cut]` window.
pub open spec fn captured_image_weight(
    event: CapturedImageEvent,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
) -> int {
    if event.row_identity == row_identity
        && event.row_image == row_image
        && captured_event_in_window(event, source_incarnation, previous_cut, new_cut)
    {
        image_event_weight_spec(event.action)
    } else {
        0
    }
}

/// Signed multiplicity for a complete-image bucket over any finite captured
/// history prefix.
pub open spec fn captured_image_bucket_delta(
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
) -> int
    decreases count,
{
    if count <= 0 || count > history.len() {
        0
    } else {
        let prefix = captured_image_bucket_delta(
            history, count - 1, row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        );
        let event_weight = captured_image_weight(
            history[count - 1], row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        );
        prefix + event_weight
    }
}

pub proof fn append_prefix_bucket_delta_is_unchanged(
    history: Seq<CapturedImageEvent>,
    appended: CapturedImageEvent,
    count: int,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
)
    requires 0 <= count <= history.len(),
    ensures
        captured_image_bucket_delta(
            history.push(appended), count, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        ) == captured_image_bucket_delta(
            history, count, row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        ),
    decreases count,
{
    if count > 0 {
        reveal(captured_image_bucket_delta);
        assert(history.push(appended).len() == history.len() + 1);
        assert(history.push(appended)[count - 1] == history[count - 1]);
        append_prefix_bucket_delta_is_unchanged(
            history, appended, count - 1, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        );
    }
}

/// Appending one captured event adds exactly its signed weight to its full-row
/// bucket; this is the sequence-level sum used by grouped SQL reduction.
pub proof fn append_captured_image_bucket_delta(
    history: Seq<CapturedImageEvent>,
    appended: CapturedImageEvent,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
)
    ensures
        captured_image_bucket_delta(
            history.push(appended), history.len() as int + 1, row_identity,
            row_image, source_incarnation, previous_cut, new_cut,
        ) == captured_image_bucket_delta(
            history, history.len() as int, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        ) + captured_image_weight(
            appended, row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        ),
{
    reveal(captured_image_bucket_delta);
    assert(history.push(appended).len() == history.len() + 1);
    assert(history.push(appended)[history.len() as int] == appended);
    append_prefix_bucket_delta_is_unchanged(
        history, appended, history.len() as int, row_identity, row_image,
        source_incarnation, previous_cut, new_cut,
    );
}

/// All captured rows from one committed source transaction share a single
/// LSN, so a consumer cut cannot admit only part of that transaction.
pub proof fn transaction_window_membership_is_atomic(
    first: CapturedImageEvent,
    second: CapturedImageEvent,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
)
    requires
        first.transaction_id == second.transaction_id,
        first.source_incarnation == second.source_incarnation,
        first.commit_lsn == second.commit_lsn,
    ensures
        captured_event_in_window(first, source_incarnation, previous_cut, new_cut)
            == captured_event_in_window(second, source_incarnation, previous_cut, new_cut),
{
}

/// A same-key payload replacement contributes a delete and insert to distinct
/// complete-image buckets; key equality alone cannot cancel the history.
pub proof fn payload_replacement_preserves_complete_image_delta(
    row_identity: Seq<u8>,
    old_image: Seq<u8>,
    new_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
    delete_lsn: u64,
    insert_lsn: u64,
)
    requires
        old_image != new_image,
        previous_cut < delete_lsn <= new_cut,
        previous_cut < insert_lsn <= new_cut,
    ensures
        captured_image_weight(
            CapturedImageEvent {
                row_identity, row_image: old_image, source_incarnation,
                transaction_id: 1, commit_lsn: delete_lsn, action: ImageEvent::Delete,
            },
            row_identity, old_image, source_incarnation, previous_cut, new_cut,
        ) == -1,
        captured_image_weight(
            CapturedImageEvent {
                row_identity, row_image: new_image, source_incarnation,
                transaction_id: 2, commit_lsn: insert_lsn, action: ImageEvent::Insert,
            },
            row_identity, new_image, source_incarnation, previous_cut, new_cut,
        ) == 1,
        captured_image_weight(
            CapturedImageEvent {
                row_identity, row_image: old_image, source_incarnation,
                transaction_id: 1, commit_lsn: delete_lsn, action: ImageEvent::Delete,
            },
            row_identity, new_image, source_incarnation, previous_cut, new_cut,
        ) == 0,
{
}

/// An insert/delete pair for the same complete image cancels only when both
/// events belong to the same source incarnation and selected consumer window.
pub proof fn transient_same_image_cancels_within_incarnation(
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
    insert_lsn: u64,
    delete_lsn: u64,
)
    requires
        previous_cut < insert_lsn <= new_cut,
        previous_cut < delete_lsn <= new_cut,
    ensures
        captured_image_weight(
            CapturedImageEvent {
                row_identity, row_image, source_incarnation,
                transaction_id: 1, commit_lsn: insert_lsn, action: ImageEvent::Insert,
            },
            row_identity, row_image, source_incarnation, previous_cut, new_cut,
        )
            + captured_image_weight(
                CapturedImageEvent {
                    row_identity, row_image, source_incarnation,
                    transaction_id: 2, commit_lsn: delete_lsn, action: ImageEvent::Delete,
                },
                row_identity, row_image, source_incarnation, previous_cut, new_cut,
            ) == 0,
{
}

/// Identical keys and images from different source incarnations remain
/// separate histories; only the selected incarnation contributes to a cut.
pub proof fn source_incarnation_prevents_cross_epoch_cancellation(
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    old_incarnation: u64,
    current_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
    old_lsn: u64,
    current_lsn: u64,
)
    requires
        old_incarnation != current_incarnation,
        previous_cut < old_lsn <= new_cut,
        previous_cut < current_lsn <= new_cut,
    ensures
        captured_image_weight(
            CapturedImageEvent {
                row_identity, row_image, source_incarnation: old_incarnation,
                transaction_id: 1, commit_lsn: old_lsn, action: ImageEvent::Delete,
            },
            row_identity, row_image, current_incarnation, previous_cut, new_cut,
        ) == 0,
        captured_image_weight(
            CapturedImageEvent {
                row_identity, row_image, source_incarnation: current_incarnation,
                transaction_id: 2, commit_lsn: current_lsn, action: ImageEvent::Insert,
            },
            row_identity, row_image, current_incarnation, previous_cut, new_cut,
        ) == 1,
{
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
        !can_compact_history_spec(true, false, false),
{
    assert(earlier_cut <= shared_cut);
    assert(shared_cut <= later_cut);
    assert(image_event_weight_spec(ImageEvent::Insert) == 1);
    assert(image_event_weight_spec(ImageEvent::Delete) == -1);
    assert(!can_compact_history_spec(true, false, false));
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
