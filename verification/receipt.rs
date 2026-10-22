//! Executable acknowledgement limit for a validated complete receipt prefix.
//!
//! PostgreSQL checks `upto_nchanges` after each whole decoded transaction and
//! stops when the emitted count reaches that limit. The adapter must supply
//! a prefix ending in COMMIT, with at least BEGIN and COMMIT, and compare the
//! consumed sequence with the durable receipts before marking them acknowledged.
//! This arithmetic proof does not establish SQL selection, decoding or durability.

use vstd::prelude::*;

verus! {
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
    use super::receipt_ack_row_limit;

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
