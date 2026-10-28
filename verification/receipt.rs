//! Executable acknowledgement limit for a validated complete receipt prefix.
//!
//! PostgreSQL checks `upto_nchanges` after each whole decoded transaction and
//! stops when the emitted count reaches that limit. The adapter must supply
//! a prefix ending in COMMIT, with at least BEGIN and COMMIT, and compare the
//! consumed sequence with the durable receipts before marking them acknowledged.
//! This arithmetic proof does not establish SQL selection, decoding or durability.

use vstd::prelude::*;

verus! {
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameMarker { Begin, Commit, Body, Malformed }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError { Invalid(usize), MalformedLsn(usize), Incomplete }

/// Immutable adapter facts; strings and PostgreSQL reads remain outside this proof.
#[allow(dead_code)]
pub struct FrameMessage {
    pub slot: Seq<char>,
    pub xid: Option<Seq<char>>,
    pub receipt_id: i64,
    pub marker: FrameMarker,
    pub position: Option<u64>,
}

pub open spec fn valid_frame_row(rows: Seq<FrameMessage>, i: int) -> bool {
    0 <= i < rows.len() && rows[i].xid.is_some() && rows[i].slot == rows[0].slot
    && (i == 0 || rows[i - 1].receipt_id < rows[i].receipt_id)
}

pub open spec fn closed_frame_row(rows: Seq<FrameMessage>, start: int, end: int, i: int) -> bool {
    valid_frame_row(rows, i)
        && rows[i].xid == rows[start].xid
        && (start < i < end - 1 ==> rows[i].marker == FrameMarker::Body)
        && rows[i].position.is_some()
        && rows[i].position->0 <= rows[end - 1].position->0
}

#[verifier::opaque]
pub open spec fn frame_span(rows: Seq<FrameMessage>, start: int, end: int) -> bool {
    0 <= start && start + 2 <= end <= rows.len()
    && rows[start].marker == FrameMarker::Begin
    && rows[end - 1].marker == FrameMarker::Commit
    && rows[end - 1].position.is_some()
    && forall|i: int| start <= i < end ==> #[trigger] closed_frame_row(rows, start, end, i)
}

#[verifier::opaque]
pub open spec fn framed_prefix(rows: Seq<FrameMessage>, ends: Seq<usize>, through: int) -> bool {
    0 <= through <= rows.len()
    && (if ends.len() == 0 { through == 0 } else { ends[ends.len() - 1] == through })
    && forall|t: int| 0 <= t < ends.len() ==> #[trigger] prefix_transaction(rows, ends, t)
}

pub open spec fn prefix_transaction(rows: Seq<FrameMessage>, ends: Seq<usize>, t: int) -> bool {
    let start = if t == 0 { 0 } else { ends[t - 1] as int };
    frame_span(rows, start, ends[t] as int)
    && (t > 0 ==> rows[start - 1].position->0 < rows[ends[t] as int - 1].position->0)
}

pub proof fn empty_framed_prefix(rows: Seq<FrameMessage>)
    ensures framed_prefix(rows, Seq::empty(), 0),
{
    reveal(framed_prefix);
}

#[verifier::rlimit(20)]
pub proof fn extend_framed_prefix(rows: Seq<FrameMessage>, ends: Seq<usize>, start: int, end: usize)
    requires framed_prefix(rows, ends, start), frame_span(rows, start, end as int),
        0 <= start < end <= rows.len(),
        start == 0 || rows[start - 1].position->0 < rows[end as int - 1].position->0,
    ensures framed_prefix(rows, ends.push(end), end as int),
{
    reveal(framed_prefix);
    assert forall|t: int| 0 <= t < ends.push(end).len() implies #[trigger] prefix_transaction(rows, ends.push(end), t) by {
        if t == ends.len() {
            assert(ends.push(end)[t] == end);
            assert((if t == 0 { 0 } else { ends.push(end)[t - 1] as int }) == start);
            if t > 0 {
                assert(start > 0) by {
                    assert(prefix_transaction(rows, ends, t - 1));
                    reveal(frame_span);
                }
            }
            assert(prefix_transaction(rows, ends.push(end), t));
        } else {
            assert(prefix_transaction(rows, ends, t));
            assert(ends.push(end)[t] == ends[t]);
            if t > 0 { assert(ends.push(end)[t - 1] == ends[t - 1]); }
            assert(prefix_transaction(rows, ends.push(end), t));
        }
    }
}

pub open spec fn open_frame_row(rows: Seq<FrameMessage>, start: int, i: int) -> bool {
    valid_frame_row(rows, i) && rows[i].xid == rows[start].xid
    && rows[i].marker == (if i == start { FrameMarker::Begin } else { FrameMarker::Body })
}

pub open spec fn open_frame(rows: Seq<FrameMessage>, start: int, next: int) -> bool {
    forall|i: int| start <= i < next ==> #[trigger] open_frame_row(rows, start, i)
}

/// Every method must read the same borrowed immutable sequence, without SPI.
pub trait ReceiptFrameSource {
    spec fn messages(&self) -> Seq<FrameMessage>;
    fn message_count(&self) -> (length: usize)
        ensures length == self.messages().len();
    fn check_row(&self, index: usize) -> (valid: bool)
        requires index < self.messages().len(),
        ensures valid == valid_frame_row(self.messages(), index as int);
    fn same_xid(&self, start: usize, index: usize) -> (same: bool)
        requires start <= index < self.messages().len(),
        ensures same == (self.messages()[start as int].xid == self.messages()[index as int].xid);
    fn marker(&self, index: usize) -> (marker: FrameMarker)
        requires index < self.messages().len(),
        ensures marker == self.messages()[index as int].marker;
    fn position(&self, index: usize) -> (position: Option<u64>)
        requires index < self.messages().len(),
        ensures position == self.messages()[index as int].position;
}

/// Successful output partitions the entire input into complete ordered transactions.
#[allow(clippy::collapsible_if)]
#[verifier::loop_isolation(false)]
#[verifier::rlimit(20)]
pub fn receipt_frame_ends<S: ReceiptFrameSource>(source: &S) -> (result: Result<Vec<usize>, FrameError>)
    ensures match result {
        Ok(ends) => framed_prefix(source.messages(), ends@, source.messages().len() as int),
        Err(FrameError::Invalid(i)) => i < source.messages().len(),
        Err(FrameError::MalformedLsn(i)) => i < source.messages().len(),
        Err(FrameError::Incomplete) => true,
    },
{
    let length = source.message_count();
    let mut ends: Vec<usize> = Vec::new();
    proof { empty_framed_prefix(source.messages()); }
    let mut start: usize = 0;
    let mut index: usize = 0;
    let mut previous_commit: Option<u64> = None;
    while index < length
        invariant
            start <= index <= length, length == source.messages().len(),
            framed_prefix(source.messages(), ends@, start as int),
            previous_commit == (if start == 0 { None } else { source.messages()[start as int - 1].position }),
            (start == 0) == previous_commit.is_none(),
            open_frame(source.messages(), start as int, index as int),
        decreases length - index,
    {
        if !source.check_row(index) { return Err(FrameError::Invalid(index)); }
        if index == start {
            if !matches!(source.marker(index), FrameMarker::Begin) { return Err(FrameError::Invalid(index)); }
        } else {
            if !source.same_xid(start, index) { return Err(FrameError::Invalid(index)); }
            let marker = source.marker(index);
            if matches!(marker, FrameMarker::Commit) {
                let commit = match source.position(index) {
                    Some(position) => position,
                    None => return Err(FrameError::MalformedLsn(index)),
                };
                if let Some(previous) = previous_commit {
                    if previous >= commit { return Err(FrameError::Invalid(index)); }
                }
                let mut row = start;
                while row <= index
                    invariant
                        start < index < length, length == source.messages().len(),
                        start <= row <= index + 1,
                        source.messages()[index as int].position == Some(commit),
                        source.messages()[index as int].marker == FrameMarker::Commit,
                        source.messages()[index as int].xid == source.messages()[start as int].xid,
                        valid_frame_row(source.messages(), index as int),
                        open_frame(source.messages(), start as int, index as int),
                        forall|i: int| start <= i < row ==> source.messages()[i].position.is_some()
                            && source.messages()[i].position->0 <= commit,
                    decreases index + 1 - row,
                {
                    let position = match source.position(row) {
                        Some(position) => position,
                        None => return Err(FrameError::MalformedLsn(row)),
                    };
                    if position > commit { return Err(FrameError::Invalid(index)); }
                    row += 1;
                }
                proof {
                    assert(frame_span(source.messages(), start as int, index as int + 1)) by {
                        reveal(frame_span);
                        assert forall|i: int| start <= i < index + 1 implies #[trigger] closed_frame_row(source.messages(), start as int, index as int + 1, i) by {
                            if i < index { assert(open_frame_row(source.messages(), start as int, i)); }
                        }
                        assert(open_frame_row(source.messages(), start as int, start as int));
                    }
                    extend_framed_prefix(source.messages(), ends@, start as int, (index + 1) as usize);
                }
                ends.push(index + 1);
                start = index + 1;
                previous_commit = Some(commit);
            } else if !matches!(marker, FrameMarker::Body) {
                return Err(FrameError::Invalid(index));
            }
        }
        proof {
            assert(open_frame(source.messages(), start as int, index as int + 1)) by {
                assert forall|i: int| start <= i < index + 1 implies #[trigger] open_frame_row(source.messages(), start as int, i) by {
                    if i == index {
                        assert(open_frame_row(source.messages(), start as int, i));
                    }
                }
            }
        }
        index += 1;
    }
    if start != length { return Err(FrameError::Incomplete); }
    Ok(ends)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetiredSlotState { Absent, Owned, Mismatch }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetiredSlotAction { Keep, Forget, Drop }

pub fn retired_slot_action(owner_matches: bool, active_reference: bool, slot: RetiredSlotState) -> (action: RetiredSlotAction)
    ensures
        match action {
            RetiredSlotAction::Keep => !owner_matches || active_reference || slot == RetiredSlotState::Mismatch,
            RetiredSlotAction::Forget => owner_matches && !active_reference && slot == RetiredSlotState::Absent,
            RetiredSlotAction::Drop => owner_matches && !active_reference && slot == RetiredSlotState::Owned,
        },
{
    if !owner_matches || active_reference {
        RetiredSlotAction::Keep
    } else {
        match slot {
            RetiredSlotState::Absent => RetiredSlotAction::Forget,
            RetiredSlotState::Owned => RetiredSlotAction::Drop,
            RetiredSlotState::Mismatch => RetiredSlotAction::Keep,
        }
    }
}

// Recognition and source classification are adapter assumptions; no catalog whitelist.
pub fn may_skip_source_free_row(source_change: bool, recognized_table: bool, ordinary_dml: bool) -> (result: bool)
    ensures result == (!source_change && recognized_table && ordinary_dml),
{
    !source_change && recognized_table && ordinary_dml
}

// Keep executable comparisons identical to the arithmetic bounds in the proof.
#[allow(clippy::manual_range_contains)]
pub fn receipt_ack_row_limit(message_count: usize) -> (result: Option<i32>)
    ensures
        match result {
            Some(limit) =>
                2 <= message_count <= 2147483647
                && 0 < limit
                && limit as int == message_count as int,
            None => message_count < 2 || message_count > 2147483647,
        },
{
    if message_count < 2 || message_count > 2147483647 {
        None
    } else {
        Some(message_count as i32)
    }
}
}

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::{
        RetiredSlotAction, RetiredSlotState, may_skip_source_free_row, receipt_ack_row_limit,
        retired_slot_action,
    };

    #[test]
    fn test_retirement_requires_current_owner_unreferenced_matching_slot() {
        use RetiredSlotAction::{Drop, Forget, Keep};
        use RetiredSlotState::{Absent, Mismatch, Owned};
        for (owner, referenced, slot, expected) in [
            (true, false, Owned, Drop),
            (true, false, Absent, Forget),
            (true, false, Mismatch, Keep),
            (false, false, Owned, Keep),
            (false, false, Absent, Keep),
            (true, true, Owned, Keep),
            (true, true, Absent, Keep),
        ] {
            assert_eq!(retired_slot_action(owner, referenced, slot), expected);
        }
    }

    #[test]
    fn test_source_free_skip_never_admits_source_unrecognized_or_other_action() {
        for source in [false, true] {
            for known in [false, true] {
                for dml in [false, true] {
                    assert_eq!(
                        may_skip_source_free_row(source, known, dml),
                        !source && known && dml
                    );
                }
            }
        }
    }

    #[test]
    fn test_receipt_ack_limit_bounds_and_exact_stop() {
        for count in [2, 3, 100, 106, i32::MAX as usize] {
            let limit = receipt_ack_row_limit(count).unwrap();
            assert!(limit > 0);
            assert_eq!(limit as usize, count);
        }
        for count in [0, 1, i32::MAX as usize + 1, usize::MAX] {
            assert_eq!(receipt_ack_row_limit(count), None);
        }
    }
}
