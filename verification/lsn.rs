//! Checked PostgreSQL `pg_lsn` parsing and contract canonical formatting.
//!
//! The accepted text grammar is two ASCII hexadecimal halves, each one to
//! eight digits, with exactly one `/` separator. The resulting position is
//! `high * 2^32 + low`, represented as an unsigned 64-bit integer. Persisted
//! frontiers remain strings; callers must parse before comparing or advancing
//! progress. This module covers the LSN value contract, not CDC durability or
//! scheduler correctness beyond those checked conversions.

use std::fmt;
use vstd::prelude::*;

verus! {
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsnParseError;

/// A PostgreSQL LSN represented by its unsigned 64-bit position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

pub closed spec fn lsn_position(lsn: Lsn) -> u64 {
    lsn.0
}

pub open spec fn valid_hex_byte(byte: u8) -> bool {
    (0x30u8 <= byte && byte <= 0x39u8)
        || (0x61u8 <= byte && byte <= 0x66u8)
        || (0x41u8 <= byte && byte <= 0x46u8)
}

pub open spec fn uppercase_hex_byte(byte: u8) -> bool {
    byte < 128u8
        && valid_hex_byte(byte)
        && ((0x30u8 <= byte && byte <= 0x39u8) || (0x41u8 <= byte && byte <= 0x46u8))
}

pub proof fn uppercase_hex_properties(byte: u8)
    requires uppercase_hex_byte(byte)
    ensures byte < 128u8, valid_hex_byte(byte)
{
    reveal(uppercase_hex_byte);
    reveal(valid_hex_byte);
}

pub open spec fn hex_digit_value(byte: u8) -> int {
    if 0x30u8 <= byte && byte <= 0x39u8 {
        byte as int - 0x30
    } else if 0x61u8 <= byte && byte <= 0x66u8 {
        byte as int - 0x61 + 10
    } else {
        byte as int - 0x41 + 10
    }
}

pub open spec fn valid_hex_part(bytes: Seq<u8>) -> bool {
    0 < bytes.len() <= 8
        && forall|index: int| 0 <= index < bytes.len() ==> valid_hex_byte(bytes[index])
}

pub open spec fn hex_value(bytes: Seq<u8>) -> int
    decreases bytes.len()
{
    if bytes.len() == 0 {
        0
    } else {
        hex_value(bytes.subrange(0, bytes.len() - 1)) * 16
            + hex_digit_value(bytes[bytes.len() - 1])
    }
}

pub open spec fn pow16(length: int) -> int {
    if length <= 0 {
        1
    } else if length == 1 {
        16
    } else if length == 2 {
        256
    } else if length == 3 {
        4096
    } else if length == 4 {
        65536
    } else if length == 5 {
        1048576
    } else if length == 6 {
        16777216
    } else if length == 7 {
        268435456
    } else {
        4294967296
    }
}

pub proof fn pow16_bound(length: int)
    requires 0 <= length <= 8
    ensures pow16(length) <= 4294967296
{}

pub proof fn pow16_step(length: int)
    requires 0 <= length < 8
    ensures pow16(length + 1) == pow16(length) * 16
{}

pub proof fn hex_value_bound(bytes: Seq<u8>)
    requires
        bytes.len() <= 8,
        forall|index: int| 0 <= index < bytes.len() ==> valid_hex_byte(bytes[index]),
    ensures 0 <= hex_value(bytes) < pow16(bytes.len() as int)
    decreases bytes.len()
{
    if bytes.len() > 0 {
        let prefix = bytes.subrange(0, bytes.len() - 1);
        hex_value_bound(prefix);
        assert(valid_hex_byte(bytes[bytes.len() - 1]));
        assert(0 <= hex_digit_value(bytes[bytes.len() - 1]) < 16);
        assert(hex_value(bytes) == hex_value(prefix) * 16 + hex_digit_value(bytes[bytes.len() - 1]));
        assert(pow16(bytes.len() as int) == 16 * pow16(prefix.len() as int));
    }
}

pub proof fn hex_value_push(bytes: Seq<u8>, byte: u8)
    ensures hex_value(bytes.push(byte)) == hex_value(bytes) * 16 + hex_digit_value(byte)
{
    broadcast use vstd::seq_lib::group_seq_properties;
    let extended = bytes.push(byte);
    assert(extended.len() == bytes.len() + 1);
    assert(extended.subrange(0, bytes.len() as int) =~= bytes);
    assert(extended[bytes.len() as int] == byte);
    reveal(hex_value);
    assert(
        hex_value(extended)
            == hex_value(extended.subrange(0, extended.len() as int - 1)) * 16
                + hex_digit_value(extended[extended.len() as int - 1])
    );
    assert(extended.len() as int - 1 == bytes.len() as int);
    assert(extended[extended.len() as int - 1] == byte);
}

pub proof fn hex_value_prepend(byte: u8, suffix: Seq<u8>)
    requires suffix.len() < 8
    ensures
        hex_value(Seq::<u8>::empty().push(byte).add(suffix))
            == hex_digit_value(byte) * pow16(suffix.len() as int) + hex_value(suffix)
    decreases suffix.len()
{
    broadcast use vstd::seq_lib::group_seq_properties;
    if suffix.len() == 0 {
        let one: Seq<u8> = Seq::empty().push(byte);
        let prefixed: Seq<u8> = one.add(suffix);
        assert(suffix =~= Seq::<u8>::empty());
        assert(prefixed =~= one);
        assert(one.len() == 1);
        assert(one[0] == byte);
        assert(prefixed.len() == 1);
        assert(prefixed[0] == byte);
        assert(prefixed.subrange(0, 0) =~= Seq::<u8>::empty());
        reveal(hex_value);
        assert(hex_value(prefixed)
            == hex_value(prefixed.subrange(0, 0)) * 16 + hex_digit_value(prefixed[0]));
        assert(hex_value(prefixed.subrange(0, 0)) == 0);
        assert(hex_digit_value(prefixed[0]) == hex_digit_value(byte));
        assert(hex_value(prefixed) == hex_digit_value(byte));
        assert(hex_value(suffix) == 0);
        assert(pow16(0) == 1);
        assert(
            hex_value(prefixed)
                == hex_digit_value(byte) * pow16(suffix.len() as int) + hex_value(suffix)
        );
    } else {
        let prefix = suffix.subrange(0, suffix.len() as int - 1);
        let last = suffix[suffix.len() as int - 1];
        let one: Seq<u8> = Seq::empty().push(byte);
        let prefixed: Seq<u8> = one.add(suffix);
        let prefixed_prefix: Seq<u8> = one.add(prefix);
        assert(suffix =~= prefix.push(last));
        assert(prefixed =~= prefixed_prefix.push(last));
        hex_value_prepend(byte, prefix);
        hex_value_push(prefixed_prefix, last);
        assert(
            hex_value(suffix)
                == hex_value(prefix) * 16 + hex_digit_value(last)
        ) by {
            reveal(hex_value);
        }
        pow16_step(suffix.len() as int - 1);
        assert(pow16(suffix.len() as int) == pow16(prefix.len() as int) * 16);
        assert(hex_value(prefixed_prefix)
            == hex_digit_value(byte) * pow16(prefix.len() as int) + hex_value(prefix));
        assert(hex_value(prefixed)
            == hex_value(prefixed_prefix) * 16 + hex_digit_value(last));
        assert(
            hex_value(prefixed)
                == hex_digit_value(byte) * pow16(suffix.len() as int) + hex_value(suffix)
        ) by (nonlinear_arith)
            requires
                hex_value(prefixed_prefix)
                    == hex_digit_value(byte) * pow16(prefix.len() as int) + hex_value(prefix),
                hex_value(prefixed)
                    == hex_value(prefixed_prefix) * 16 + hex_digit_value(last),
                hex_value(suffix) == hex_value(prefix) * 16 + hex_digit_value(last),
                pow16(suffix.len() as int) == pow16(prefix.len() as int) * 16;
    }
}

pub proof fn hex_value_extensional(left: Seq<u8>, right: Seq<u8>)
    requires left =~= right
    ensures hex_value(left) == hex_value(right)
    decreases left.len()
{
    broadcast use vstd::seq_lib::group_seq_properties;
    assert(left.len() == right.len());
    if left.len() > 0 {
        let left_prefix = left.subrange(0, left.len() as int - 1);
        let right_prefix = right.subrange(0, right.len() as int - 1);
        assert(left_prefix =~= right_prefix);
        assert(left[left.len() as int - 1] == right[right.len() as int - 1]);
        hex_value_extensional(left_prefix, right_prefix);
        reveal(hex_value);
        assert(hex_value(left)
            == hex_value(left_prefix) * 16 + hex_digit_value(left[left.len() as int - 1]));
        assert(hex_value(right)
            == hex_value(right_prefix) * 16 + hex_digit_value(right[right.len() as int - 1]));
    }
}

pub open spec fn valid_lsn_at(bytes: Seq<u8>, separator: int) -> bool {
    0 < separator < bytes.len() - 1
        && bytes[separator] == 0x2Fu8
        && separator <= 8
        && bytes.len() - separator - 1 <= 8
        && forall|index: int|
            0 <= index < bytes.len() && index != separator ==> valid_hex_byte(bytes[index])
}

pub open spec fn valid_lsn_bytes(bytes: Seq<u8>) -> bool {
    exists|separator: int| valid_lsn_at(bytes, separator)
}

pub proof fn lsn_non_hex_byte_invalid(bytes: Seq<u8>, index: int)
    requires
        0 <= index < bytes.len(),
        bytes[index] != 0x2Fu8,
        !valid_hex_byte(bytes[index]),
    ensures !valid_lsn_bytes(bytes)
{
    if valid_lsn_bytes(bytes) {
        let separator = choose|separator: int| valid_lsn_at(bytes, separator);
        assert(separator != index);
        assert(valid_hex_byte(bytes[index]));
        assert(false);
    }
}

pub proof fn lsn_two_separators_invalid(bytes: Seq<u8>, first: int, second: int)
    requires
        0 <= first < second < bytes.len(),
        bytes[first] == 0x2Fu8,
        bytes[second] == 0x2Fu8,
    ensures !valid_lsn_bytes(bytes)
{
    if valid_lsn_bytes(bytes) {
        let separator = choose|separator: int| valid_lsn_at(bytes, separator);
        if separator == first {
            assert(separator != second);
            assert(valid_hex_byte(bytes[second]));
        } else {
            assert(separator != first);
            assert(valid_hex_byte(bytes[first]));
        }
        assert(false);
    }
}

pub proof fn lsn_invalid_shape(bytes: Seq<u8>, separator: int)
    requires
        0 <= separator < bytes.len(),
        bytes[separator] == 0x2Fu8,
        forall|index: int|
            0 <= index < bytes.len() && index != separator ==> valid_hex_byte(bytes[index]),
        separator == 0 || separator >= bytes.len() - 1 || separator > 8
            || bytes.len() - separator - 1 > 8,
    ensures !valid_lsn_bytes(bytes)
{
    if valid_lsn_bytes(bytes) {
        let valid_separator = choose|valid_separator: int| valid_lsn_at(bytes, valid_separator);
        if valid_separator != separator {
            assert(valid_hex_byte(bytes[separator]));
            assert(false);
        }
        assert(!(separator == 0 || separator >= bytes.len() - 1 || separator > 8
            || bytes.len() - separator - 1 > 8));
        assert(false);
    }
}

pub proof fn lsn_no_separator_invalid(bytes: Seq<u8>)
    requires forall|index: int| 0 <= index < bytes.len() ==> bytes[index] != 0x2Fu8
    ensures !valid_lsn_bytes(bytes)
{
    if valid_lsn_bytes(bytes) {
        let separator = choose|separator: int| valid_lsn_at(bytes, separator);
        assert(bytes[separator] == 0x2Fu8);
        assert(false);
    }
}

pub open spec fn lsn_bytes_value(bytes: Seq<u8>) -> int {
    let separator = choose|index: int| valid_lsn_at(bytes, index);
    hex_value(bytes.subrange(0, separator)) * 4294967296
        + hex_value(bytes.subrange(separator + 1, bytes.len() as int))
}

pub proof fn lsn_bytes_value_at(bytes: Seq<u8>, separator: int)
    requires valid_lsn_at(bytes, separator)
    ensures lsn_bytes_value(bytes)
        == hex_value(bytes.subrange(0, separator)) * 4294967296
            + hex_value(bytes.subrange(separator + 1, bytes.len() as int))
{
    let chosen = choose|index: int| valid_lsn_at(bytes, index);
    if chosen != separator {
        assert(valid_hex_byte(bytes[separator]));
        assert(false);
    }
}

pub fn pack_halves(high: u64, low: u64) -> (position: u64)
    requires high < 4_294_967_296 && low < 4_294_967_296
    ensures position as int == high as int * 4294967296 + low as int
{
    high * 4_294_967_296_u64 + low
}

#[allow(dead_code)]
pub fn lsn_parse_value_and_bounds(bytes: &[u8]) -> (result: Result<Lsn, LsnParseError>)
    ensures
        match result {
            Ok(parsed) => valid_lsn_bytes(bytes@)
                && lsn_position(parsed) as int == lsn_bytes_value(bytes@)
                && lsn_position(parsed) <= u64::MAX,
            Err(_) => !valid_lsn_bytes(bytes@),
        }
{
    let result = Lsn::parse_bytes_prefix(bytes, bytes.len());
    proof {
        broadcast use vstd::seq_lib::group_seq_properties;
        assert(bytes@.subrange(0, bytes.len() as int) =~= bytes@);
    }
    result
}

pub fn numeric_gt(left: Lsn, right: Lsn) -> (greater: bool)
    ensures greater == (lsn_position(left) as int > lsn_position(right) as int)
{
    left.position() > right.position()
}

pub fn numeric_gte(left: Lsn, right: Lsn) -> (greater_or_equal: bool)
    ensures greater_or_equal == (lsn_position(left) as int >= lsn_position(right) as int)
{
    left.position() >= right.position()
}

#[allow(dead_code)]
pub fn lsn_numeric_order(left: Lsn, right: Lsn) -> (greater: bool)
    ensures greater == (lsn_position(left) as int > lsn_position(right) as int)
{
    numeric_gt(left, right)
}

pub fn hex_character(nibble: u8) -> (byte: u8)
    requires nibble <= 15
    ensures valid_hex_byte(byte), uppercase_hex_byte(byte), hex_digit_value(byte) == nibble as int
{
    if nibble < 10 {
        0x30u8 + nibble
    } else {
        0x41u8 + nibble - 10
    }
}

pub fn format_bytes(position: u64) -> (formatted: ([u8; 17], usize))
    ensures
        formatted.1 <= 17,
        forall|index: int| 0 <= index < formatted.1 ==> formatted.0@[index] < 128,
        forall|index: int| 0 <= index < formatted.1
            ==> formatted.0@[index] == 0x2Fu8 || uppercase_hex_byte(formatted.0@[index]),
        formatted.0@[formatted.1 as int - 9] == 0x2Fu8,
        ((position >> 32) as u32 == 0 ==> formatted.1 == 10)
            && ((position >> 32) as u32 != 0 ==> formatted.0@[0] != 0x30u8),
        valid_lsn_bytes(formatted.0@.subrange(0, formatted.1 as int)),
        lsn_bytes_value(formatted.0@.subrange(0, formatted.1 as int)) == position as int
{
    assert((position >> 32) <= u32::MAX as u64) by (bit_vector);
    let high = (position >> 32) as u32;
    let mut high_value = high;
    let mut buffer = [0_u8; 17];
    let mut high_digits = 0_usize;
    if (position >> 32) as u32 == 0 {
        buffer[0] = 0x30u8;
        high_digits = 1;
        assert(high as int == 0);
        assert(hex_digit_value(buffer@[0]) == 0);
        let ghost zero_high_bytes = buffer@.subrange(0, 1);
        assert(zero_high_bytes.len() == 1);
        assert(zero_high_bytes[0] == 0x30u8);
        assert(zero_high_bytes.subrange(0, 0) =~= Seq::<u8>::empty());
        reveal(hex_value);
        assert(hex_value(zero_high_bytes.subrange(0, 0)) == 0);
        assert(hex_value(zero_high_bytes)
            == hex_value(zero_high_bytes.subrange(0, 0)) * 16
                + hex_digit_value(zero_high_bytes[0]));
        assert(hex_value(zero_high_bytes) == 0);
        assert(high_digits == 1);
        assert(buffer@.subrange(0, high_digits as int) =~= zero_high_bytes);
        proof {
            hex_value_extensional(buffer@.subrange(0, high_digits as int), zero_high_bytes);
        }
        assert(hex_value(buffer@.subrange(0, high_digits as int)) == high as int);
    } else {
        assert((high as int) < 4294967296);
        while high_value > 0
            invariant
                high_digits <= 8,
                high_value == 0 ==> high_digits > 0,
                high_value == 0 ==> buffer@[8 - high_digits as int] != 0x30u8,
                high_value as int * pow16(high_digits as int) <= high as int,
                high_value as int * pow16(high_digits as int)
                    + hex_value(buffer@.subrange(8 - high_digits as int, 8)) == high as int,
                forall|digit_index: int| 8 - high_digits as int <= digit_index < 8
                    ==> #[trigger] uppercase_hex_byte(buffer@[digit_index]),
            decreases high_value,
        {
            if high_digits == 8 {
                proof {
                    assert(pow16(high_digits as int) == 4294967296);
                    assert(high_value as int >= 1);
                    assert(high_value as int * pow16(high_digits as int) <= high as int);
                    assert((high as int) < 4294967296);
                    assert(high_value as int * 4294967296 >= 4294967296)
                        by (nonlinear_arith)
                        requires high_value as int >= 1;
                    assert(false);
                }
            }
            assert(high_digits < 8);
            assert(7 - high_digits < 17);
            let digit_value = high_value % 16;
            assert(digit_value < 16);
            let ghost old_value = high_value;
            let ghost old_digits = high_digits;
            let ghost old_suffix = buffer@.subrange(8 - high_digits as int, 8);
            let digit_byte = hex_character(digit_value as u8);
            buffer[7 - high_digits] = digit_byte;
            let next_value = high_value / 16;
            proof {
                let new_suffix = buffer@.subrange(7 - old_digits as int, 8);
                assert(new_suffix =~= Seq::<u8>::empty().push(digit_byte).add(old_suffix));
                hex_value_prepend(digit_byte, old_suffix);
                assert(high_value as int == next_value as int * 16 + digit_value as int);
                assert(next_value as int * 16 <= high_value as int);
                assert(hex_digit_value(digit_byte) == digit_value as int);
                assert(old_suffix.len() == old_digits as int);
                pow16_step(old_digits as int);
                assert(hex_value(new_suffix)
                    == digit_value as int * pow16(old_digits as int) + hex_value(old_suffix));
                assert(old_value == high_value);
                assert(
                    next_value as int * pow16((old_digits + 1) as int)
                        + hex_value(new_suffix)
                        == old_value as int * pow16(old_digits as int) + hex_value(old_suffix)
                ) by (nonlinear_arith)
                    requires
                        next_value as int * 16 + digit_value as int == old_value as int,
                        pow16((old_digits + 1) as int)
                            == pow16(old_digits as int) * 16,
                        hex_value(new_suffix)
                            == digit_value as int * pow16(old_digits as int)
                                + hex_value(old_suffix);
                assert(next_value as int * pow16((old_digits + 1) as int) <= high as int)
                    by (nonlinear_arith)
                    requires
                        next_value as int * 16 <= old_value as int,
                        pow16((old_digits + 1) as int)
                            == pow16(old_digits as int) * 16,
                        old_value as int * pow16(old_digits as int) <= high as int,
                        pow16(old_digits as int) >= 0;
                if next_value == 0 {
                    assert(old_value > 0);
                    assert(old_value < 16);
                    assert(digit_value == old_value);
                    assert(digit_value > 0);
                    assert(digit_byte != 0x30u8);
                    assert(8 - (old_digits + 1) as int == 7 - old_digits as int);
                    assert(buffer@[8 - (old_digits + 1) as int] == digit_byte);
                }
            }
            high_value = next_value;
            high_digits += 1;
        }
        let high_start = 8 - high_digits;
        let ghost high_suffix = buffer@.subrange(high_start as int, 8);
        assert(high_start as int == 8 - high_digits as int);
        assert(high_suffix.len() == 8 - high_start as int);
        assert(high_suffix.len() == high_digits as int);
        assert(high_digits > 0);
        assert(high_suffix[0] == buffer@[high_start as int]);
        assert(buffer@[high_start as int] != 0x30u8);
        assert(high_value == 0);
        assert(
            high_value as int * pow16(high_digits as int)
                + hex_value(buffer@.subrange(8 - high_digits as int, 8))
                == high as int
        );
        assert(hex_value(high_suffix) == high as int);
        let mut index = 0_usize;
        while index < high_digits
            invariant
                index <= high_digits,
                high_digits <= 8,
                high_suffix.len() == high_digits as int,
                high_start == 8 - high_digits,
                forall|digit_index: int| 0 <= digit_index < index as int
                    ==> uppercase_hex_byte(buffer@[digit_index]),
                forall|digit_index: int| high_start as int <= digit_index < 8
                    ==> uppercase_hex_byte(buffer@[digit_index]),
                forall|digit_index: int| 0 <= digit_index < index as int
                    ==> buffer@[digit_index] == high_suffix[digit_index],
                forall|digit_index: int| index as int <= digit_index < high_digits as int
                    ==> buffer@[high_start as int + digit_index]
                        == high_suffix[digit_index],
            decreases high_digits - index,
        {
            assert(index < high_digits);
            assert(high_suffix.len() == high_digits as int);
            assert((index as int) < high_suffix.len());
            assert(high_start + index < 8);
            let ghost old_index = index;
            let ghost digit_byte = buffer[high_start + index];
            assert(digit_byte == high_suffix[old_index as int]);
            buffer[index] = buffer[high_start + index];
            assert(buffer@[old_index as int] == digit_byte);
            index += 1;
        }
        assert(index == high_digits);
        assert(index as int == high_suffix.len());
        assert(buffer@.subrange(0, index as int) =~= high_suffix);
        assert(hex_value(buffer@.subrange(0, index as int)) == hex_value(high_suffix));
        assert(hex_value(buffer@.subrange(0, high_digits as int)) == high as int);
    }
    assert(1 <= high_digits <= 8);
    if (position >> 32) as u32 == 0 {
        assert(high_digits == 1);
    } else {
        assert(high_digits > 0);
        assert(buffer@[0] != 0x30u8);
    }
    assert(hex_value(buffer@.subrange(0, high_digits as int)) == high as int);
    assert((position >> 32) as u32 == 0 ==> high_digits + 9 == 10);
    assert((position >> 32) as u32 != 0 ==> buffer@[0] != 0x30u8);
    assert(forall|digit_index: int| 0 <= digit_index < high_digits as int
        ==> uppercase_hex_byte(buffer@[digit_index]));
    let ghost high_prefix = buffer@.subrange(0, high_digits as int);
    assert(hex_value(high_prefix) == high as int);
    buffer[high_digits] = 0x2Fu8;
    assert(buffer@.subrange(0, high_digits as int) =~= high_prefix);
    let low = position as u32;
    let mut low_value = low;
    let mut index = 0_usize;
    while index < 8
            invariant
                index <= 8,
                high_digits <= 8,
                low_value as int * pow16(index as int) <= low as int,
                buffer@.subrange(0, high_digits as int) =~= high_prefix,
                low_value as int * pow16(index as int)
                    + hex_value(buffer@.subrange(
                        high_digits as int + 9 - index as int,
                        high_digits as int + 9,
                    )) == low as int,
                forall|digit_index: int| 0 <= digit_index < high_digits as int
                    ==> uppercase_hex_byte(buffer@[digit_index]),
                buffer@[high_digits as int] == 0x2Fu8,
                ((position >> 32) as u32 != 0 ==> buffer@[0] != 0x30u8),
                forall|digit_index: int| high_digits as int + 9 - index as int <= digit_index
                    && digit_index < high_digits as int + 9
                    ==> #[trigger] uppercase_hex_byte(buffer@[digit_index]),
        decreases 8 - index,
    {
        assert(index < 8);
        assert(8 - index > 0);
        let output_index = high_digits + 8 - index;
        assert(high_digits < output_index);
        assert(output_index < 17);
        let digit_value = low_value % 16;
        assert(digit_value < 16);
        let ghost old_value = low_value;
        let ghost old_index = index;
        let ghost old_suffix = buffer@.subrange(
            high_digits as int + 9 - index as int,
            high_digits as int + 9,
        );
        let digit_byte = hex_character(digit_value as u8);
        buffer[output_index] = digit_byte;
        assert(buffer@.subrange(0, high_digits as int) =~= high_prefix);
        let next_value = low_value / 16;
        proof {
            let new_suffix = buffer@.subrange(
                high_digits as int + 8 - old_index as int,
                high_digits as int + 9,
            );
            assert(new_suffix =~= Seq::<u8>::empty().push(digit_byte).add(old_suffix));
            hex_value_prepend(digit_byte, old_suffix);
            assert(old_value as int == next_value as int * 16 + digit_value as int);
            pow16_step(old_index as int);
            assert(
                next_value as int * pow16((old_index + 1) as int)
                    + hex_value(new_suffix)
                    == (next_value as int * 16 + digit_value as int)
                        * pow16(old_index as int) + hex_value(old_suffix)
            ) by (nonlinear_arith)
                requires
                    old_value as int == next_value as int * 16 + digit_value as int,
                    hex_value(new_suffix)
                        == digit_value as int * pow16(old_index as int)
                            + hex_value(old_suffix),
                    pow16((old_index + 1) as int)
                        == pow16(old_index as int) * 16;
            assert(old_value == low_value);
            assert(next_value as int * pow16((old_index + 1) as int) <= low as int)
                by (nonlinear_arith)
                requires
                    next_value as int * 16 <= old_value as int,
                    pow16((old_index + 1) as int) == pow16(old_index as int) * 16,
                    old_value as int * pow16(old_index as int) <= low as int,
                    pow16(old_index as int) >= 0;
        }
        low_value = next_value;
        index += 1;
    }
    assert(pow16(8) == 4294967296);
    if low_value > 0 {
        assert(low_value as int * pow16(8) >= 4294967296)
            by (nonlinear_arith)
            requires low_value as int >= 1;
        assert(low_value as int * pow16(8) <= low as int);
        assert((low as int) < 4294967296);
        assert(false);
    }
    assert(low_value == 0);
    proof {
        hex_value_extensional(buffer@.subrange(0, high_digits as int), high_prefix);
    }
    assert(hex_value(buffer@.subrange(0, high_digits as int)) == high as int);
    assert(hex_value(buffer@.subrange(high_digits as int + 1, high_digits as int + 9)) == low as int);
    assert(forall|digit_index: int| 0 <= digit_index < 8
        ==> #[trigger] uppercase_hex_byte(buffer@[high_digits as int + 1 + digit_index]));
    assert(forall|digit_index: int| 0 <= digit_index < high_digits as int
        ==> uppercase_hex_byte(buffer@[digit_index]));
    assert forall|output_index: int| 0 <= output_index < high_digits as int + 9
        implies buffer@[output_index] < 128
    by {
        if output_index < high_digits as int {
            uppercase_hex_properties(buffer@[output_index]);
        } else if output_index == high_digits as int {
            assert(buffer@[output_index] == 0x2Fu8);
        } else {
            assert(high_digits as int + 1 <= output_index < high_digits as int + 9);
            uppercase_hex_properties(buffer@[output_index]);
        }
    }
    assert forall|output_index: int| 0 <= output_index < high_digits as int + 9
        implies buffer@[output_index] == 0x2Fu8 || uppercase_hex_byte(buffer@[output_index])
    by {
        if output_index < high_digits as int {
            assert(uppercase_hex_byte(buffer@[output_index]));
        } else if output_index == high_digits as int {
            assert(buffer@[output_index] == 0x2Fu8);
        } else {
            assert(high_digits as int + 1 <= output_index < high_digits as int + 9);
            assert(uppercase_hex_byte(buffer@[output_index]));
        }
    }
    assert(buffer@[high_digits as int] == 0x2Fu8);
    let ghost output = buffer@.subrange(0, high_digits as int + 9);
    assert(valid_lsn_at(output, high_digits as int)) by {
        assert(0 < high_digits as int);
        assert((high_digits as int) < output.len() - 1);
        assert(high_digits as int <= 8);
        assert(output.len() - high_digits as int - 1 == 8);
        assert forall|output_index: int| 0 <= output_index < output.len()
                && output_index != high_digits as int
            implies valid_hex_byte(output[output_index])
        by {
            if output_index < high_digits as int {
                uppercase_hex_properties(output[output_index]);
            } else {
                uppercase_hex_properties(output[output_index]);
            }
        }
    }
    assert(valid_lsn_bytes(output));
    proof {
        lsn_bytes_value_at(output, high_digits as int);
    }
    assert(output.len() == high_digits as int + 9);
    assert(output.subrange(0, high_digits as int)
        =~= buffer@.subrange(0, high_digits as int));
    proof {
        hex_value_extensional(
            output.subrange(0, high_digits as int),
            buffer@.subrange(0, high_digits as int),
        );
    }
    assert(output.subrange(high_digits as int + 1, output.len() as int)
        =~= buffer@.subrange(high_digits as int + 1, high_digits as int + 9));
    assert(hex_value(output.subrange(0, high_digits as int)) == high as int);
    assert(hex_value(output.subrange(high_digits as int + 1, output.len() as int)) == low as int);
    assert(lsn_bytes_value(output)
        == high as int * 4294967296 + low as int);
    assert(
        position
            == ((((position >> 32) as u32) as u64) << 32) + ((position as u32) as u64)
    ) by (bit_vector);
    assert(((high as u64) << 32) == (high as u64) * 4_294_967_296_u64)
        by (bit_vector);
    assert(position == ((high as u64) << 32) + low as u64);
    assert(position as int == high as int * 4294967296 + low as int);
    assert(lsn_bytes_value(output) == position as int);
    let formatted = (buffer, high_digits + 9);
    assert(formatted.0@[0] == buffer@[0]);
    assert((position >> 32) as u32 == 0 ==> formatted.1 == 10);
    assert((position >> 32) as u32 != 0 ==> formatted.0@[0] != 0x30u8);
    formatted
}

#[allow(dead_code)]
pub fn lsn_format_parse_roundtrip(position: u64) -> (parsed: Result<Lsn, LsnParseError>)
    ensures match parsed {
        Ok(lsn) => lsn_position(lsn) == position,
        Err(_) => false,
    }
{
    let (formatted, length) = format_bytes(position);
    Lsn::parse_bytes_prefix(&formatted, length)
}

pub fn hex_digit(byte: u8) -> (result: Option<u8>)
    ensures match result {
        Some(digit) => valid_hex_byte(byte) && hex_digit_value(byte) == digit as int,
        None => !valid_hex_byte(byte),
    }
{
    match byte {
        0x30u8..=0x39u8 => Some(byte - 0x30u8),
        0x61u8..=0x66u8 => Some(byte - 0x61u8 + 10),
        0x41u8..=0x46u8 => Some(byte - 0x41u8 + 10),
        _ => None,
    }
}

pub fn parse_half(
    value: &[u8],
    start: usize,
    end: usize,
) -> (parsed: u64)
    requires
        start < end,
        end <= value.len(),
        end - start <= 8,
        forall|prefix_index: int|
            start as int <= prefix_index && prefix_index < end as int
                ==> valid_hex_byte(value@[prefix_index]),
    ensures
        parsed as int == hex_value(value@.subrange(start as int, end as int)),
        (parsed as int) < 4294967296,
{
    let mut parsed = 0_u64;
    let mut index = start;
    while index < end
        invariant
            start <= index <= end,
            end <= value.len(),
            end - start <= 8,
            index <= value.len(),
            index < end ==> index < value.len(),
            index - start <= 8,
            index - start + (end - index) == end - start,
            forall|prefix_index: int|
                start as int <= prefix_index && prefix_index < end as int
                    ==> valid_hex_byte(value@[prefix_index]),
            forall|prefix_index: int|
                start as int <= prefix_index && prefix_index < index as int
                    ==> valid_hex_byte(value@[prefix_index]),
            parsed as int == hex_value(value@.subrange(start as int, index as int)),
            (parsed as int) < pow16((index - start) as int),
        decreases end - index,
    {
        assert(index < value.len());
        assert(valid_hex_byte(value@[index as int]));
        let digit = match hex_digit(value[index]) {
            Some(digit) => u64::from(digit),
            None => 0_u64,
        };
        proof {
            assert(value@.subrange(start as int, index as int).len() <= 8);
            hex_value_bound(value@.subrange(start as int, index as int));
            assert(end - index > 0);
            assert(index - start + (end - index) == end - start);
            assert(end - start <= 8);
            assert(index - start + 1 <= index - start + (end - index));
            assert(index - start + (end - index) <= 8);
            assert(index - start + 1 <= 8);
            pow16_bound((index - start + 1) as int);
            assert(digit as int == hex_digit_value(value@[index as int]));
            let prefix = value@.subrange(start as int, index as int);
            let extended = value@.subrange(start as int, (index + 1) as int);
            assert(extended == prefix.push(value@[index as int]));
            hex_value_push(prefix, value@[index as int]);
            assert(
                hex_value(extended)
                    == hex_value(value@.subrange(start as int, index as int)) * 16
                        + hex_digit_value(value@[index as int])
            );
            assert((parsed as int * 16 + digit as int) < pow16((index - start + 1) as int));
            assert(pow16((index - start + 1) as int) <= 4294967296);
        }
        parsed = parsed * 16 + digit;
        index += 1;
    }
    proof {
        pow16_bound((end - start) as int);
    }
    parsed
}

impl Lsn {
    pub const fn from_position(position: u64) -> Self {
        Self(position)
    }

    pub const fn position(self) -> (position: u64)
        ensures position == lsn_position(self)
    {
        self.0
    }

    pub fn parse_bytes_prefix(value: &[u8], length: usize) -> (result: Result<Self, LsnParseError>)
        requires length <= value.len()
        ensures
            match result {
                Ok(_) => valid_lsn_bytes(value@.subrange(0, length as int)),
                Err(_) => !valid_lsn_bytes(value@.subrange(0, length as int)),
            },
            match result {
                Ok(parsed) => lsn_position(parsed) as int
                    == lsn_bytes_value(value@.subrange(0, length as int)),
                Err(_) => true,
            }
    {
        let mut separator = length;
        let mut found_separator = false;
        let mut index = 0;
        while index < length
            invariant
                index <= length,
                length <= value.len(),
                found_separator == exists|prefix_index: int|
                    0 <= prefix_index < index as int && value@[prefix_index] == 0x2Fu8,
                found_separator ==> separator < index && value@[separator as int] == 0x2Fu8,
                forall|prefix_index: int|
                    0 <= prefix_index < index as int
                        ==> value@[prefix_index] == 0x2Fu8 || valid_hex_byte(value@[prefix_index]),
                forall|prefix_index: int|
                    0 <= prefix_index < index as int && prefix_index != separator as int
                        ==> value@[prefix_index] != 0x2Fu8,
            decreases length - index,
        {
            if value[index] == 0x2Fu8 {
                if found_separator {
                    proof {
                        lsn_two_separators_invalid(
                            value@.subrange(0, length as int),
                            separator as int,
                            index as int,
                        );
                    }
                    return Err(LsnParseError);
                }
                separator = index;
                found_separator = true;
            } else if hex_digit(value[index]).is_none() {
                proof {
                    lsn_non_hex_byte_invalid(value@.subrange(0, length as int), index as int);
                }
                return Err(LsnParseError);
            }
            index += 1;
        }
        if !found_separator {
            proof {
                assert(forall|prefix_index: int|
                    0 <= prefix_index < length as int ==> value@[prefix_index] != 0x2Fu8);
                lsn_no_separator_invalid(value@.subrange(0, length as int));
            }
            return Err(LsnParseError);
        }
        if length < 2 || separator == 0 || separator >= length - 1
            || separator > 8 || length - separator - 1 > 8
        {
            proof {
                lsn_invalid_shape(value@.subrange(0, length as int), separator as int);
            }
            return Err(LsnParseError);
        }
        assert(forall|prefix_index: int|
            0 <= prefix_index < length as int && prefix_index != separator as int
                ==> valid_hex_byte(value@[prefix_index]));
        assert(valid_lsn_at(value@.subrange(0, length as int), separator as int));
        assert(valid_lsn_bytes(value@.subrange(0, length as int)));
        assert(forall|prefix_index: int|
            0 <= prefix_index && prefix_index < separator as int
                ==> valid_hex_byte(value@[prefix_index]));
        assert(forall|prefix_index: int|
            separator as int + 1 <= prefix_index && prefix_index < length as int
                ==> valid_hex_byte(value@[prefix_index]));
        let high = parse_half(value, 0, separator);
        let low = parse_half(value, separator + 1, length);
        let position = pack_halves(high, low);
        let parsed = Self(position);
        proof {
            let prefix = value@.subrange(0, length as int);
            let high_bytes = prefix.subrange(0, separator as int);
            let low_bytes = prefix.subrange(separator as int + 1, prefix.len() as int);
            broadcast use vstd::seq_lib::group_seq_properties;
            assert(prefix.len() == length as int);
            assert(high_bytes =~= value@.subrange(0, separator as int));
            assert(
                low_bytes =~= value@.subrange(separator as int + 1, length as int)
            );
            lsn_bytes_value_at(prefix, separator as int);
            assert(high as int == hex_value(high_bytes));
            assert(low as int == hex_value(low_bytes));
            assert(
                lsn_bytes_value(prefix)
                    == hex_value(high_bytes) * 4294967296 + hex_value(low_bytes)
            );
            assert(lsn_position(parsed) == position);
            assert(position as int == high as int * 4294967296 + low as int);
            assert(lsn_bytes_value(prefix) == high as int * 4294967296 + low as int);
        }
        Ok(parsed)
    }
}

#[allow(dead_code)]
fn main() {}
}

impl Lsn {
    pub fn parse_bytes(value: &[u8]) -> Result<Self, LsnParseError> {
        Self::parse_bytes_prefix(value, value.len())
    }

    /// Parse PostgreSQL's two-component hexadecimal `pg_lsn` spelling.
    pub fn parse(value: &str) -> Result<Self, LsnParseError> {
        // Grammar validation and numeric conversion are both performed by the verified byte parser.
        Self::parse_bytes(value.as_bytes())
    }
}

impl fmt::Display for LsnParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("expected two hexadecimal LSN components of 1-8 digits separated by '/'")
    }
}

impl std::error::Error for LsnParseError {}

impl Lsn {
    /// Format in the contract's canonical uppercase `X/XXXXXXXX` spelling.
    pub fn format(self) -> String {
        let (bytes, length) = format_bytes(self.position());
        // SAFETY: `format_bytes` is verified to emit ASCII bytes in the returned prefix.
        unsafe { String::from_utf8_unchecked(bytes[..length].to_vec()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_lsn_checked_parse_rejects_malformed_and_overflow() {
        for value in [
            "",
            "/1",
            "1/",
            "1/2/3",
            "1/2\n",
            "+1/2",
            "100000000/0",
            "0/100000000",
            "0/G",
        ] {
            assert!(Lsn::parse(value).is_err(), "accepted {value:?}");
        }
        assert_eq!(
            Lsn::parse("FFFFFFFF/FFFFFFFF").unwrap().position(),
            u64::MAX
        );
    }

    #[test]
    fn test_lsn_parse_format_round_trip() {
        for value in [0, 1, 1_u64 << 32, u64::MAX] {
            let formatted = Lsn::from_position(value).format();
            assert_eq!(Lsn::parse(&formatted).unwrap().position(), value);
        }
        assert_eq!(Lsn::from_position(0).format(), "0/00000000");
        assert_eq!(Lsn::from_position(1_u64 << 32).format(), "1/00000000");
        assert_eq!(Lsn::from_position(u64::MAX).format(), "FFFFFFFF/FFFFFFFF");
    }

    proptest! {
        #[test]
        fn test_lsn_parse_format_round_trip_all_positions(position in any::<u64>()) {
            let formatted = Lsn::from_position(position).format();
            prop_assert_eq!(Lsn::parse(&formatted).map(Lsn::position), Ok(position));
        }
    }
}
