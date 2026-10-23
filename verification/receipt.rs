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
