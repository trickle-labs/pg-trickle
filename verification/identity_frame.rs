//! Bounded immutable scalar identity framing; payload canonicalization is external.
use vstd::prelude::*;

verus! {
pub open spec fn scalar_width(tag: u8) -> int {
    if tag == 1 { 1 } else if tag == 2 { 2 } else if tag == 3 { 4 }
    else if tag == 4 { 8 } else if tag == 13 { 16 } else { 0 }
}

pub fn fixed_scalar_width(tag: u8) -> (width: usize)
    ensures width as int == scalar_width(tag), width <= 16,
{
    match tag { 1 => 1, 2 => 2, 3 => 4, 4 => 8, 13 => 16, _ => 0 }
}

/// The complete ordinary tuple limit, including header, tags and terminator.
pub fn checked_frame_size(field_count: usize, payload_bytes: usize) -> (size: Option<usize>)
    ensures
        size.is_some() == (field_count <= 1024 && 7 + 2 * (field_count as int) + payload_bytes <= 1048576),
        match size { None => true, Some(n) => n as int == 7 + 2 * (field_count as int) + payload_bytes && n <= 1048576 },
{
    if field_count > 1024 || payload_bytes > 1048576 { return None; }
    let size = 7 + 2 * field_count + payload_bytes;
    if size > 1048576 { None } else { Some(size) }
}

/// Independent one-field grammar: exact header, typed state, bounded body, final byte.
pub open spec fn scalar_wire(s: Seq<u8>) -> bool {
    9 <= s.len() <= 25
    && (s[0] == 2 || s[0] == 3) && 1 <= s[1] <= 8
    && s[2] == 0 && s[3] == 0 && s[4] == 0 && s[5] == 1
    && scalar_width(s[6]) > 0
    && s[s.len() - 1] == 255
    && (if s[7] == 0 { s.len() == 9 } else {
        s[7] == 1 && s.len() == 9 + scalar_width(s[6])
        && (s[6] != 1 || s[8] <= 1)
    })
}

pub open spec fn represents(s: Seq<u8>, version: u8, domain: u8, tag: u8, payload: Option<Seq<u8>>) -> bool {
    scalar_wire(s) && s[0] == version && s[1] == domain && s[6] == tag
    && match payload {
        None => s[7] == 0,
        Some(bytes) => s[7] == 1 && s.subrange(8, s.len() as int - 1) == bytes,
    }
}

pub open spec fn valid_input(version: u8, domain: u8, tag: u8, payload: Option<Seq<u8>>) -> bool {
    (version == 2 || version == 3) && 1 <= domain <= 8 && scalar_width(tag) > 0
    && match payload {
        None => true,
        Some(bytes) => bytes.len() == scalar_width(tag) && (tag != 1 || bytes[0] <= 1),
    }
}

pub fn validate_scalar_frame(input: &[u8]) -> (valid: bool)
    ensures valid == scalar_wire(input@),
{
    if input.len() < 9 || input.len() > 25 { return false; }
    if (input[0] != 2 && input[0] != 3) || input[1] < 1 || input[1] > 8
        || input[2] != 0 || input[3] != 0 || input[4] != 0 || input[5] != 1 {
        return false;
    }
    let width = fixed_scalar_width(input[6]);
    if width == 0 || input[input.len() - 1] != 255 { return false; }
    if input[7] == 0 { return input.len() == 9; }
    input[7] == 1 && input.len() == 9 + width && (input[6] != 1 || input[8] <= 1)
}

pub fn encode_scalar_frame(version: u8, domain: u8, tag: u8, payload: Option<&[u8]>) -> (encoded: Option<Vec<u8>>)
    ensures
        encoded.is_some() == valid_input(version, domain, tag, match payload { None => None, Some(p) => Some(p@) }),
        match encoded {
            None => true,
            Some(out) => represents(out@, version, domain, tag, match payload { None => None, Some(p) => Some(p@) }),
        },
{
    let width = fixed_scalar_width(tag);
    if (version != 2 && version != 3) || width == 0 { return None; }
    match domain { 1..=8 => {}, _ => return None }
    if let Some(bytes) = payload {
        if bytes.len() != width { return None; }
        if tag == 1 && bytes[0] > 1 { return None; }
    }
    let mut out = Vec::with_capacity(if payload.is_some() { 9 + width } else { 9 });
    out.push(version);
    out.push(domain);
    out.push(0);
    out.push(0);
    out.push(0);
    out.push(1);
    out.push(tag);
    out.push(if payload.is_some() { 1 } else { 0 });
    if let Some(bytes) = payload {
        let mut i: usize = 0;
        while i < bytes.len()
            invariant
                bytes.len() == width, width <= 16,
                width as int == scalar_width(tag), width > 0,
                version == 2 || version == 3, 1 <= domain <= 8,
                tag != 1 || bytes@[0] <= 1,
                i <= bytes.len(), out.len() == 8 + i,
                out@[0] == version, out@[1] == domain,
                out@[2] == 0, out@[3] == 0, out@[4] == 0, out@[5] == 1,
                out@[6] == tag, out@[7] == 1,
                forall|j: int| 0 <= j < i ==> out@[8 + j] == bytes@[j],
            decreases bytes.len() - i,
        {
            out.push(bytes[i]);
            i += 1;
        }
        proof { assert(out@.subrange(8, out.len() as int) =~= bytes@); }
    }
    out.push(255);
    proof {
        if let Some(bytes) = payload {
            assert(out@.subrange(8, out.len() as int - 1) =~= bytes@);
        }
    }
    Some(out)
}

/// A frame's boundaries determine version, domain, type, NULL state and body.
pub proof fn unique_scalar_decoding(s: Seq<u8>, v1: u8, d1: u8, t1: u8, p1: Option<Seq<u8>>,
    v2: u8, d2: u8, t2: u8, p2: Option<Seq<u8>>)
    requires represents(s, v1, d1, t1, p1), represents(s, v2, d2, t2, p2),
    ensures v1 == v2, d1 == d2, t1 == t2, p1 == p2,
{}
}

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_frames_preserve_payloads_and_reject_truncation_and_trailing_bytes() {
        for version in [2, 3] {
            for domain in 1..=8 {
                for (tag, width) in [(1, 1), (2, 2), (3, 4), (4, 8), (13, 16)] {
                    for payload in [
                        None,
                        Some(vec![0; width]),
                        Some(vec![if tag == 1 { 1 } else { 255 }; width]),
                    ] {
                        let frame =
                            encode_scalar_frame(version, domain, tag, payload.as_deref()).unwrap();
                        assert!(validate_scalar_frame(&frame));
                        assert_eq!(
                            &frame[..8],
                            &[
                                version,
                                domain,
                                0,
                                0,
                                0,
                                1,
                                tag,
                                u8::from(payload.is_some())
                            ]
                        );
                        assert_eq!(
                            &frame[8..frame.len() - 1],
                            payload.as_deref().unwrap_or(&[])
                        );
                        for end in 0..frame.len() {
                            assert!(
                                !validate_scalar_frame(&frame[..end]),
                                "truncated at {end}: {frame:?}"
                            );
                        }
                        for byte in [0, 255] {
                            let mut trailing = frame.clone();
                            trailing.push(byte);
                            assert!(!validate_scalar_frame(&trailing));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn malformed_headers_types_states_and_payloads_are_rejected() {
        let frame = vec![2, 1, 0, 0, 0, 1, 1, 1, 1, 255];
        for (offset, byte) in [
            (0, 0),
            (0, 4),
            (1, 0),
            (1, 9),
            (2, 1),
            (3, 1),
            (4, 1),
            (5, 0),
            (5, 2),
            (6, 0),
            (6, 5),
            (6, 255),
            (7, 2),
            (7, 255),
            (8, 2),
            (9, 0),
        ] {
            let mut malformed = frame.clone();
            malformed[offset] = byte;
            assert!(
                !validate_scalar_frame(&malformed),
                "offset {offset}, byte {byte}"
            );
        }
        assert!(encode_scalar_frame(2, 1, 1, Some(&[2])).is_none());
        assert!(encode_scalar_frame(2, 1, 3, Some(&[0; 3])).is_none());
        assert!(encode_scalar_frame(2, 1, 3, Some(&[0; 5])).is_none());
        assert!(encode_scalar_frame(2, 1, 3, Some(&[])).is_none());
        assert!(encode_scalar_frame(1, 1, 3, None).is_none());
        assert!(encode_scalar_frame(2, 0, 3, None).is_none());
        assert!(encode_scalar_frame(2, 1, 255, None).is_none());
    }

    #[test]
    fn capacity_checks_cover_exact_limits_and_machine_overflow() {
        assert_eq!(checked_frame_size(0, 1048576 - 7), Some(1048576));
        assert_eq!(checked_frame_size(1024, 1048576 - 2055), Some(1048576));
        assert_eq!(checked_frame_size(1024, 1048576 - 2054), None);
        for (count, bytes) in [
            (1025, 0),
            (usize::MAX, 0),
            (1, usize::MAX),
            (usize::MAX, usize::MAX),
        ] {
            assert_eq!(checked_frame_size(count, bytes), None);
        }
    }
}
