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

/// Keyed endpoint reduction prefers an inserted final image to a deleted old image.
pub fn image_action_priority(action: ImageAction) -> (priority: u8)
    ensures
        (action == ImageAction::Insert) ==> priority == 0,
        (action == ImageAction::Delete) ==> priority == 1,
        (action == ImageAction::Skip) ==> priority == 2,
{
    match action {
        ImageAction::Insert => 0,
        ImageAction::Delete => 1,
        ImageAction::Skip => 2,
    }
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

/// Whether one event belongs to a selected identity's source-incarnation cut.
/// Other identities may be interleaved in the global captured history; keyed
/// reduction must keep their image buckets independent.
pub open spec fn identity_event_in_window(
    event: CapturedImageEvent,
    row_identity: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
) -> bool {
    event.row_identity == row_identity
        && captured_event_in_window(
            event, source_incarnation, previous_cut, new_cut,
        )
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
    if identity_event_in_window(
        event, row_identity, source_incarnation, previous_cut, new_cut,
    ) && event.row_image == row_image
    {
        image_event_weight_spec(event.action)
    } else {
        0
    }
}

/// A row with another complete identity cannot add or cancel weight in this
/// identity's image bucket, even when its image and consumer window match.
pub proof fn different_identity_has_zero_image_weight(
    event: CapturedImageEvent,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
)
    requires event.row_identity != row_identity,
    ensures captured_image_weight(
        event, row_identity, row_image, source_incarnation, previous_cut, new_cut,
    ) == 0,
{
    reveal(captured_image_weight);
    reveal(identity_event_in_window);
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

/// Apply the selected history prefix to one complete-image bucket, starting
/// with its existing bag multiplicity. Multiple logical rows may share an
/// image, so the count is an integer multiplicity rather than row presence.
pub open spec fn captured_image_count_after(
    initial_multiplicity: int,
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
        initial_multiplicity
    } else {
        captured_image_count_after(
            initial_multiplicity, history, count - 1, row_identity,
            row_image, source_incarnation, previous_cut, new_cut,
        ) + captured_image_weight(
            history[count - 1], row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        )
    }
}

/// A captured DELETE is valid for this bag bucket only when an occurrence of
/// that complete image was present immediately before the event. INSERTs and
/// events outside the selected source/window do not require a prior image.
pub open spec fn valid_captured_image_bucket_history(
    initial_multiplicity: int,
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
) -> bool
    decreases count,
{
    if count <= 0 {
        true
    } else if count > history.len() {
        false
    } else {
        let event_weight = captured_image_weight(
            history[count - 1], row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        );
        let before = captured_image_count_after(
            initial_multiplicity, history, count - 1, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        );
        valid_captured_image_bucket_history(
            initial_multiplicity, history, count - 1, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        ) && (event_weight != -1 || before > 0)
    }
}

/// A valid finite event prefix never drives a complete-image bag bucket below
/// zero. This admits repeated occurrences while rejecting unmatched deletes.
pub proof fn valid_captured_image_bucket_history_is_nonnegative(
    initial_multiplicity: int,
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
)
    requires
        initial_multiplicity >= 0,
        0 <= count <= history.len(),
        previous_cut <= new_cut,
        valid_captured_image_bucket_history(
            initial_multiplicity, history, count, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        ),
    ensures
        captured_image_count_after(
            initial_multiplicity, history, count, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        ) >= 0,
    decreases count,
{
    if count > 0 {
        reveal(valid_captured_image_bucket_history);
        valid_captured_image_bucket_history_is_nonnegative(
            initial_multiplicity, history, count - 1, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        );
        reveal(captured_image_count_after);
        let event_weight = captured_image_weight(
            history[count - 1], row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        );
        let before = captured_image_count_after(
            initial_multiplicity, history, count - 1, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        );
        assert(-1 <= event_weight <= 1) by {
            reveal(captured_image_weight);
            reveal(captured_event_in_window);
            reveal(image_event_in_window_spec);
            reveal(image_event_weight_spec);
        }
        if event_weight == -1 {
            assert(before > 0);
        }
        assert(before + event_weight >= 0);
    }
}

/// For any finite event history, the signed SQL bucket sum is exactly the
/// final bag multiplicity minus its initial multiplicity. This permits
/// duplicate rows and makes the accumulator semantics explicit independently
/// of the one-row transition proof below.
pub proof fn captured_image_bucket_delta_matches_bag_count(
    initial_multiplicity: int,
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
)
    requires
        initial_multiplicity >= 0,
        0 <= count <= history.len(),
        previous_cut <= new_cut,
    ensures
        captured_image_count_after(
            initial_multiplicity, history, count, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        ) == initial_multiplicity + captured_image_bucket_delta(
            history, count, row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        ),
    decreases count,
{
    reveal(captured_image_count_after);
    reveal(captured_image_bucket_delta);
    if count > 0 {
        captured_image_bucket_delta_matches_bag_count(
            initial_multiplicity, history, count - 1, row_identity,
            row_image, source_incarnation, previous_cut, new_cut,
        );
        assert(captured_image_count_after(
            initial_multiplicity, history, count, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        ) == captured_image_count_after(
            initial_multiplicity, history, count - 1, row_identity, row_image,
            source_incarnation, previous_cut, new_cut,
        ) + captured_image_weight(
            history[count - 1], row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        ));
        assert(captured_image_bucket_delta(
            history, count, row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        ) == captured_image_bucket_delta(
            history, count - 1, row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        ) + captured_image_weight(
            history[count - 1], row_identity, row_image, source_incarnation,
            previous_cut, new_cut,
        ));
    }
}

/// The state before an event when walking a valid one-row history backward.
pub open spec fn previous_image_state(
    after: Option<Seq<u8>>,
    event: CapturedImageEvent,
) -> Option<Seq<u8>> {
    match (after, event.action) {
        (Some(_), ImageEvent::Insert) => None,
        (None, ImageEvent::Delete) => Some(event.row_image),
        _ => after,
    }
}

/// A valid projection of a global history onto one unique row identity and
/// consumer window, traversed from its final state. Events for other identities,
/// source incarnations or cuts are interleaved but do not change this row's state.
pub open spec fn valid_history_from_state(
    after: Option<Seq<u8>>,
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
) -> bool
    decreases count,
{
    if count <= 0 {
        true
    } else if count > history.len() {
        false
    } else {
        let event = history[count - 1];
        if identity_event_in_window(
            event, row_identity, source_incarnation, previous_cut, new_cut,
        ) {
            (match (after, event.action) {
                (Some(after_image), ImageEvent::Insert) => after_image == event.row_image,
                (None, ImageEvent::Delete) => true,
                _ => false,
            }) && valid_history_from_state(
                previous_image_state(after, event), history, count - 1,
                row_identity, source_incarnation, previous_cut, new_cut,
            )
        } else {
            valid_history_from_state(
                after, history, count - 1,
                row_identity, source_incarnation, previous_cut, new_cut,
            )
        }
    }
}

/// Reconstruct one identity's row image before a history prefix, ignoring
/// interleaved events outside its source-incarnation and consumer window.
pub open spec fn image_state_before(
    after: Option<Seq<u8>>,
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
) -> Option<Seq<u8>>
    decreases count,
{
    if count <= 0 || count > history.len() {
        after
    } else {
        let event = history[count - 1];
        if identity_event_in_window(event, row_identity, source_incarnation, previous_cut, new_cut) {
            image_state_before(
                previous_image_state(after, event), history, count - 1,
                row_identity, source_incarnation, previous_cut, new_cut,
            )
        } else {
            image_state_before(
                after, history, count - 1, row_identity, source_incarnation,
                previous_cut, new_cut,
            )
        }
    }
}

pub open spec fn image_presence(state: Option<Seq<u8>>, image: Seq<u8>) -> int {
    if state == Some(image) { 1 } else { 0 }
}

/// For a valid unique-row history, each complete-image bucket's signed event
/// sum equals that image's final multiplicity minus its initial multiplicity.
pub proof fn valid_single_identity_history_preserves_image_delta(
    final_state: Option<Seq<u8>>,
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
    image: Seq<u8>,
)
    requires
        0 <= count <= history.len(),
        previous_cut <= new_cut,
        valid_history_from_state(
            final_state, history, count, row_identity, source_incarnation,
            previous_cut, new_cut,
        ),
    ensures
        captured_image_bucket_delta(
            history, count, row_identity, image, source_incarnation,
            previous_cut, new_cut,
        ) == image_presence(final_state, image)
            - image_presence(image_state_before(
                final_state, history, count, row_identity, source_incarnation,
                previous_cut, new_cut,
            ), image),
    decreases count,
{
    if count > 0 {
        let event = history[count - 1];
        let selected = identity_event_in_window(
            event, row_identity, source_incarnation, previous_cut, new_cut,
        );
        let before = if selected { previous_image_state(final_state, event) } else { final_state };
        reveal(valid_history_from_state);
        reveal(captured_image_bucket_delta);
        valid_single_identity_history_preserves_image_delta(
            before, history, count - 1, row_identity, source_incarnation,
            previous_cut, new_cut, image,
        );
        assert(image_state_before(
            final_state, history, count, row_identity, source_incarnation,
            previous_cut, new_cut,
        ) == image_state_before(
            before, history, count - 1, row_identity, source_incarnation,
            previous_cut, new_cut,
        ));
        assert(captured_image_bucket_delta(
            history, count, row_identity, image, source_incarnation,
            previous_cut, new_cut,
        ) == captured_image_bucket_delta(
            history, count - 1, row_identity, image, source_incarnation,
            previous_cut, new_cut,
        ) + captured_image_weight(
            event, row_identity, image, source_incarnation, previous_cut, new_cut,
        ));
        if selected {
            match (final_state, event.action) {
                (Some(after_image), ImageEvent::Insert) => {
                    assert(after_image == event.row_image);
                    assert(before == Option::<Seq<u8>>::None);
                    assert(captured_image_weight(
                        event, row_identity, image, source_incarnation,
                        previous_cut, new_cut,
                    ) == image_presence(final_state, image) - image_presence(before, image));
                }
                (None, ImageEvent::Delete) => {
                    assert(before == Some(event.row_image));
                    assert(captured_image_weight(
                        event, row_identity, image, source_incarnation,
                        previous_cut, new_cut,
                    ) == image_presence(final_state, image) - image_presence(before, image));
                }
                _ => assert(false),
            }
        } else {
            assert(captured_image_weight(
                event, row_identity, image, source_incarnation, previous_cut, new_cut,
            ) == 0);
            assert(image_presence(final_state, image) == image_presence(before, image));
        }
    }
}

/// A valid unique-identity history has at most one positive and one negative
/// complete-image delta, exactly the shape selected by the keyed SQL adapter.
pub proof fn valid_history_has_unique_signed_image_endpoints(
    final_state: Option<Seq<u8>>,
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
    first_image: Seq<u8>,
    second_image: Seq<u8>,
)
    requires
        0 <= count <= history.len(),
        previous_cut <= new_cut,
        valid_history_from_state(
            final_state, history, count, row_identity, source_incarnation,
            previous_cut, new_cut,
        ),
    ensures
        (captured_image_bucket_delta(
            history, count, row_identity, first_image, source_incarnation,
            previous_cut, new_cut,
        ) > 0 && captured_image_bucket_delta(
            history, count, row_identity, second_image, source_incarnation,
            previous_cut, new_cut,
        ) > 0) ==> first_image == second_image,
        (captured_image_bucket_delta(
            history, count, row_identity, first_image, source_incarnation,
            previous_cut, new_cut,
        ) < 0 && captured_image_bucket_delta(
            history, count, row_identity, second_image, source_incarnation,
            previous_cut, new_cut,
        ) < 0) ==> first_image == second_image,
{
    valid_single_identity_history_preserves_image_delta(
        final_state, history, count, row_identity, source_incarnation,
        previous_cut, new_cut, first_image,
    );
    valid_single_identity_history_preserves_image_delta(
        final_state, history, count, row_identity, source_incarnation,
        previous_cut, new_cut, second_image,
    );
    let initial_state = image_state_before(
        final_state, history, count, row_identity, source_incarnation,
        previous_cut, new_cut,
    );
    let first_delta = captured_image_bucket_delta(
        history, count, row_identity, first_image, source_incarnation,
        previous_cut, new_cut,
    );
    let second_delta = captured_image_bucket_delta(
        history, count, row_identity, second_image, source_incarnation,
        previous_cut, new_cut,
    );
    assert(first_delta == image_presence(final_state, first_image)
        - image_presence(initial_state, first_image));
    assert(second_delta == image_presence(final_state, second_image)
        - image_presence(initial_state, second_image));
    reveal(image_presence);
    if first_delta > 0 && second_delta > 0 {
        match final_state {
            Some(final_image) => {
                if final_image != first_image {
                    assert(image_presence(final_state, first_image) == 0);
                    assert(first_delta <= 0);
                    assert(false);
                }
                if final_image != second_image {
                    assert(image_presence(final_state, second_image) == 0);
                    assert(second_delta <= 0);
                    assert(false);
                }
                assert(first_image == final_image);
                assert(second_image == final_image);
                assert(first_image == second_image);
            },
            None => {
                assert(image_presence(final_state, first_image) == 0);
                assert(first_delta <= 0);
                assert(false);
            },
        }
    }
    if first_delta < 0 && second_delta < 0 {
        match initial_state {
            Some(initial_image) => {
                if initial_image != first_image {
                    assert(image_presence(initial_state, first_image) == 0);
                    assert(first_delta >= 0);
                    assert(false);
                }
                if initial_image != second_image {
                    assert(image_presence(initial_state, second_image) == 0);
                    assert(second_delta >= 0);
                    assert(false);
                }
                assert(first_image == initial_image);
                assert(second_image == initial_image);
                assert(first_image == second_image);
            },
            None => {
                assert(image_presence(initial_state, first_image) == 0);
                assert(first_delta >= 0);
                assert(false);
            },
        }
    }
}

/// Two independent unique rows may have the same old and new payload images
/// in one interleaved committed history. Their endpoint deltas must remain
/// partitioned by row identity, as keyed SQL partitions its image SUMs.
pub proof fn interleaved_row_identity_histories_preserve_endpoints(
    first_identity: Seq<u8>,
    second_identity: Seq<u8>,
    old_image: Seq<u8>,
    new_image: Seq<u8>,
    source_incarnation: u64,
    previous_cut: u64,
    new_cut: u64,
)
    requires
        first_identity != second_identity,
        old_image != new_image,
        previous_cut < new_cut,
    ensures
        valid_history_from_state(
            Some(new_image), interleaved_identity_history(
                first_identity, second_identity, old_image, new_image,
                source_incarnation, new_cut,
            ), 4, first_identity, source_incarnation, previous_cut, new_cut,
        ),
        valid_history_from_state(
            Some(old_image), interleaved_identity_history(
                first_identity, second_identity, old_image, new_image,
                source_incarnation, new_cut,
            ), 4, second_identity, source_incarnation, previous_cut, new_cut,
        ),
        captured_image_bucket_delta(
            interleaved_identity_history(
                first_identity, second_identity, old_image, new_image,
                source_incarnation, new_cut,
            ), 4, first_identity, old_image, source_incarnation,
            previous_cut, new_cut,
        ) == -1,
        captured_image_bucket_delta(
            interleaved_identity_history(
                first_identity, second_identity, old_image, new_image,
                source_incarnation, new_cut,
            ), 4, first_identity, new_image, source_incarnation,
            previous_cut, new_cut,
        ) == 1,
        captured_image_bucket_delta(
            interleaved_identity_history(
                first_identity, second_identity, old_image, new_image,
                source_incarnation, new_cut,
            ), 4, second_identity, old_image, source_incarnation,
            previous_cut, new_cut,
        ) == 1,
        captured_image_bucket_delta(
            interleaved_identity_history(
                first_identity, second_identity, old_image, new_image,
                source_incarnation, new_cut,
            ), 4, second_identity, new_image, source_incarnation,
            previous_cut, new_cut,
        ) == -1,
{
    let history = interleaved_identity_history(
        first_identity, second_identity, old_image, new_image,
        source_incarnation, new_cut,
    );
    assert(history.len() == 4);
    reveal(interleaved_identity_history);
    reveal_with_fuel(valid_history_from_state, 5);
    assert(valid_history_from_state(
        Some(new_image), history, 4, first_identity, source_incarnation,
        previous_cut, new_cut,
    ));
    assert(valid_history_from_state(
        Some(old_image), history, 4, second_identity, source_incarnation,
        previous_cut, new_cut,
    ));
    valid_single_identity_history_preserves_image_delta(
        Some(new_image), history, 4, first_identity, source_incarnation,
        previous_cut, new_cut, old_image,
    );
    valid_single_identity_history_preserves_image_delta(
        Some(new_image), history, 4, first_identity, source_incarnation,
        previous_cut, new_cut, new_image,
    );
    valid_single_identity_history_preserves_image_delta(
        Some(old_image), history, 4, second_identity, source_incarnation,
        previous_cut, new_cut, old_image,
    );
    valid_single_identity_history_preserves_image_delta(
        Some(old_image), history, 4, second_identity, source_incarnation,
        previous_cut, new_cut, new_image,
    );
    reveal_with_fuel(image_state_before, 5);
    assert(image_state_before(
        Some(new_image), history, 4, first_identity, source_incarnation,
        previous_cut, new_cut,
    ) == Some(old_image));
    assert(image_state_before(
        Some(old_image), history, 4, second_identity, source_incarnation,
        previous_cut, new_cut,
    ) == Some(new_image));
    reveal(image_presence);
    assert(captured_image_bucket_delta(
        history, 4, first_identity, old_image, source_incarnation,
        previous_cut, new_cut,
    ) == 0 - 1);
    assert(captured_image_bucket_delta(
        history, 4, first_identity, new_image, source_incarnation,
        previous_cut, new_cut,
    ) == 1 - 0);
    assert(captured_image_bucket_delta(
        history, 4, second_identity, old_image, source_incarnation,
        previous_cut, new_cut,
    ) == 1 - 0);
    assert(captured_image_bucket_delta(
        history, 4, second_identity, new_image, source_incarnation,
        previous_cut, new_cut,
    ) == 0 - 1);
}

/// A committed key swap with two identities deliberately interleaved by source
/// trigger order. The rows exchange images, so each image's global signed sum
/// is zero while each identity still has a distinct final endpoint.
pub open spec fn interleaved_identity_history(
    first_identity: Seq<u8>,
    second_identity: Seq<u8>,
    old_image: Seq<u8>,
    new_image: Seq<u8>,
    source_incarnation: u64,
    commit_lsn: u64,
) -> Seq<CapturedImageEvent> {
    Seq::new(4, |index: int| {
        if index == 0 {
            CapturedImageEvent {
                row_identity: first_identity, row_image: old_image,
                source_incarnation, transaction_id: 7, commit_lsn,
                action: ImageEvent::Delete,
            }
        } else if index == 1 {
            CapturedImageEvent {
                row_identity: second_identity, row_image: new_image,
                source_incarnation, transaction_id: 7, commit_lsn,
                action: ImageEvent::Delete,
            }
        } else if index == 2 {
            CapturedImageEvent {
                row_identity: first_identity, row_image: new_image,
                source_incarnation, transaction_id: 7, commit_lsn,
                action: ImageEvent::Insert,
            }
        } else {
            CapturedImageEvent {
                row_identity: second_identity, row_image: old_image,
                source_incarnation, transaction_id: 7, commit_lsn,
                action: ImageEvent::Insert,
            }
        }
    })
}

/// Splitting one consumer range at a cut preserves every image bucket's delta
/// over an arbitrary finite captured history. The two half-open-by-boundary
/// windows are disjoint because membership is `(previous_cut, new_cut]`.
pub proof fn captured_image_bucket_delta_splits_at_consumer_cut(
    history: Seq<CapturedImageEvent>,
    count: int,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    earlier_cut: u64,
    middle_cut: u64,
    later_cut: u64,
)
    requires
        0 <= count <= history.len(),
        earlier_cut <= middle_cut <= later_cut,
    ensures
        captured_image_bucket_delta(
            history, count, row_identity, row_image, source_incarnation,
            earlier_cut, later_cut,
        ) == captured_image_bucket_delta(
            history, count, row_identity, row_image, source_incarnation,
            earlier_cut, middle_cut,
        ) + captured_image_bucket_delta(
            history, count, row_identity, row_image, source_incarnation,
            middle_cut, later_cut,
        ),
    decreases count,
{
    if count > 0 {
        reveal(captured_image_bucket_delta);
        captured_image_bucket_delta_splits_at_consumer_cut(
            history, count - 1, row_identity, row_image, source_incarnation,
            earlier_cut, middle_cut, later_cut,
        );
        captured_image_weight_splits_at_consumer_cut(
            history[count - 1], row_identity, row_image, source_incarnation,
            earlier_cut, middle_cut, later_cut,
        );
    }
}

/// One event contributes to exactly one side of an adjacent consumer cut.
pub proof fn captured_image_weight_splits_at_consumer_cut(
    event: CapturedImageEvent,
    row_identity: Seq<u8>,
    row_image: Seq<u8>,
    source_incarnation: u64,
    earlier_cut: u64,
    middle_cut: u64,
    later_cut: u64,
)
    requires earlier_cut <= middle_cut <= later_cut,
    ensures
        captured_image_weight(
            event, row_identity, row_image, source_incarnation,
            earlier_cut, later_cut,
        ) == captured_image_weight(
            event, row_identity, row_image, source_incarnation,
            earlier_cut, middle_cut,
        ) + captured_image_weight(
            event, row_identity, row_image, source_incarnation,
            middle_cut, later_cut,
        ),
{
    reveal(captured_image_weight);
    reveal(captured_event_in_window);
    reveal(image_event_in_window_spec);
    if event.row_identity == row_identity
        && event.row_image == row_image
        && event.source_incarnation == source_incarnation
    {
        if event.commit_lsn > earlier_cut && event.commit_lsn <= later_cut {
            if event.commit_lsn <= middle_cut {
                assert(event.commit_lsn > earlier_cut && event.commit_lsn <= middle_cut);
                assert(!(event.commit_lsn > middle_cut && event.commit_lsn <= later_cut));
            } else {
                assert(!(event.commit_lsn > earlier_cut && event.commit_lsn <= middle_cut));
                assert(event.commit_lsn > middle_cut && event.commit_lsn <= later_cut);
            }
        }
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
    let insert_priority = image_action_priority(ImageAction::Insert);
    let delete_priority = image_action_priority(ImageAction::Delete);
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
         WHERE {signs} ORDER BY __pgt_row_id, CASE WHEN __pgt_action='I' THEN {insert_priority} ELSE {delete_priority} END)",
        signs = signs.join(" OR "),
        event_weight = event_weight,
        insert_priority = insert_priority,
        delete_priority = delete_priority,
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

    #[test]
    fn keyed_action_priority_prefers_the_final_insert_image() {
        assert_eq!(image_action_priority(ImageAction::Insert), 0);
        assert_eq!(image_action_priority(ImageAction::Delete), 1);
        assert_eq!(image_action_priority(ImageAction::Skip), 2);
    }
}
