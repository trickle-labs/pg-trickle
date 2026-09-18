//! xxHash-based row ID generation for stream tables.
//!
//! Row IDs are deterministic 64-bit hashes used to identify rows in
//! incrementally-maintained stream tables.
//!
//! ## Hash functions
//!
//! - `pg_trickle_hash`: Single-value hash using xxh64.
//! - `pg_trickle_hash_multi`: Multi-value composite hash using a versioned,
//!   length-delimited framing and the xxh3 streaming API.

use pgrx::prelude::*;
use xxhash_rust::xxh64;

/// Version of the composite row-identity byte framing.
pub(crate) const COMPOSITE_ENCODING_VERSION: u16 = 2;
/// Catalog value for the active composite row-identity encoding.
pub(crate) const CURRENT_ROW_IDENTITY_VERSION: i16 = COMPOSITE_ENCODING_VERSION as i16;
const NULL_TAG: u8 = 0;
const VALUE_TAG: u8 = 1;

/// Build the canonical SQL call used for every composite row identity.
///
/// The legacy function name is retained because it is used throughout the
/// SQL generator, but it now returns the complete V2 identity rather than a
/// lossy 64-bit digest.
pub(crate) fn build_composite_hash_expr(expressions: &[String]) -> String {
    build_row_identity_expr("SCAN_KEY", expressions)
}

/// Build a typed V2 identity expression for a named semantic domain.
pub(crate) fn build_row_identity_expr(domain: &str, expressions: &[String]) -> String {
    format!(
        "pgtrickle.encode_row_id_v2('{domain}', ROW({}))",
        expressions.join(", ")
    )
}

/// Compute a 64-bit xxHash row ID from a text representation.
///
/// This function is exposed as a SQL function for use in INSERT statements
/// and delta query generation.
///
/// NULL input is mapped to a deterministic sentinel (`\x00NULL\x00`) so that
/// rows with NULL-valued group keys receive a non-NULL `__pgt_row_id`.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
fn pg_trickle_hash(input: Option<&str>) -> i64 {
    // Use a fixed seed for deterministic hashing
    const SEED: u64 = 0x517cc1b727220a95;
    let text = input.unwrap_or("\x00NULL\x00");
    let hash = xxh64::xxh64(text.as_bytes(), SEED);
    hash as i64
}

/// Compute a row ID by hashing multiple text values.
///
/// Hash multiple text values using the versioned composite framing.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
fn pg_trickle_hash_multi(inputs: Vec<Option<String>>) -> i64 {
    use xxhash_rust::xxh3::Xxh3;
    const SEED: u64 = 0x517cc1b727220a95;

    let mut hasher = Xxh3::with_seed(SEED);
    update_framed_components(&mut hasher, &inputs);

    hasher.digest() as i64
}

fn update_framed_components(hasher: &mut xxhash_rust::xxh3::Xxh3, inputs: &[Option<String>]) {
    write_framed_components(
        inputs.len(),
        inputs.iter().map(|input| input.as_deref()),
        |bytes| {
            hasher.update(bytes);
        },
    );
}

fn write_framed_components<'a>(
    component_count: usize,
    inputs: impl IntoIterator<Item = Option<&'a str>>,
    mut write: impl FnMut(&[u8]),
) {
    write(&COMPOSITE_ENCODING_VERSION.to_be_bytes());
    write(&(component_count as u64).to_be_bytes());
    for input in inputs {
        match input {
            Some(value) => {
                write(&[VALUE_TAG]);
                write(&(value.len() as u64).to_be_bytes());
                write(value.as_bytes());
            }
            None => write(&[NULL_TAG]),
        }
    }
}

#[cfg(test)]
fn encode_framed_components(inputs: &[Option<&str>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    write_framed_components(inputs.len(), inputs.iter().copied(), |bytes| {
        encoded.extend_from_slice(bytes);
    });
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_determinism() {
        let hash1 = xxh64::xxh64(b"hello world", 0x517cc1b727220a95);
        let hash2 = xxh64::xxh64(b"hello world", 0x517cc1b727220a95);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_different_inputs() {
        let hash1 = xxh64::xxh64(b"hello", 0x517cc1b727220a95);
        let hash2 = xxh64::xxh64(b"world", 0x517cc1b727220a95);
        assert_ne!(hash1, hash2);
    }

    // ── Tests for xxh3 streaming multi-hash ────────────────────────────────

    /// Helper: compute multi-hash the same way pg_trickle_hash_multi does.
    fn multi_hash(inputs: &[Option<&str>]) -> i64 {
        use xxhash_rust::xxh3::Xxh3;
        const SEED: u64 = 0x517cc1b727220a95;
        let mut hasher = Xxh3::with_seed(SEED);
        let owned: Vec<Option<String>> = inputs
            .iter()
            .map(|input| input.map(str::to_owned))
            .collect();
        update_framed_components(&mut hasher, &owned);
        hasher.digest() as i64
    }

    #[test]
    fn test_null_handling_in_multi_hash() {
        let h1 = multi_hash(&[Some("a"), None, Some("b")]);
        let h2 = multi_hash(&[Some("a"), None, Some("c")]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_multi_hash_determinism() {
        let h1 = multi_hash(&[Some("a"), Some("b"), Some("c")]);
        let h2 = multi_hash(&[Some("a"), Some("b"), Some("c")]);
        assert_eq!(h1, h2);
    }

    // ── pg_trickle_hash() tests via raw xxh64 (same logic, avoids pg_extern) ──

    #[test]
    fn test_pg_trickle_hash_empty_string() {
        const SEED: u64 = 0x517cc1b727220a95;
        let hash = xxh64::xxh64(b"", SEED);
        // Should produce a valid non-zero hash (xxHash of empty with non-zero seed)
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_pg_trickle_hash_i64_range() {
        // Verify the cast from u64 to i64 doesn't panic
        const SEED: u64 = 0x517cc1b727220a95;
        let hash = xxh64::xxh64(b"test", SEED);
        let _ = hash as i64; // Should not panic
    }

    // ── pg_trickle_hash_multi() framing ────────────────────────────────────

    #[test]
    fn test_multi_hash_separator_prevents_collision() {
        // "ab" + "c" vs "a" + "bc" — length framing differentiates them.
        let h1 = multi_hash(&[Some("ab"), Some("c")]);
        let h2 = multi_hash(&[Some("a"), Some("bc")]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_multi_hash_null_vs_string_null() {
        // NULL and the literal string "NULL" have distinct tags.
        let h1 = multi_hash(&[None]);
        let h2 = multi_hash(&[Some("NULL")]);
        assert_ne!(
            h1, h2,
            "NULL marker and string 'NULL' should hash differently"
        );
        assert_ne!(h1, multi_hash(&[Some("")]));
    }

    #[test]
    fn test_multi_hash_many_values() {
        let h = multi_hash(&[Some("1"), Some("2"), Some("3"), None, Some("5")]);
        let _ = h; // valid i64, no panic
    }

    #[test]
    fn test_framing_has_fixed_header_and_tags() {
        let bytes = encode_framed_components(&[None, Some("")]);
        assert_eq!(&bytes[..2], &COMPOSITE_ENCODING_VERSION.to_be_bytes());
        assert_eq!(&bytes[2..10], &2_u64.to_be_bytes());
        assert_eq!(bytes[10], NULL_TAG);
        assert_eq!(bytes[11], VALUE_TAG);
        assert_eq!(&bytes[12..20], &0_u64.to_be_bytes());
    }

    #[test]
    fn test_framing_component_boundaries_and_arity_are_distinct() {
        assert_ne!(
            encode_framed_components(&[Some("ab"), Some("c")]),
            encode_framed_components(&[Some("a"), Some("bc")])
        );
        assert_ne!(
            encode_framed_components(&[Some("a")]),
            encode_framed_components(&[Some("a"), Some("")])
        );
    }

    #[test]
    fn test_framing_separator_like_control_and_unicode_values() {
        let values = [Some("\x1e\x00"), Some("naïve 🦀")];
        let encoded = encode_framed_components(&values);
        assert!(encoded.windows(2).any(|window| window == b"\x1e\0"));
        assert!(encoded.ends_with("naïve 🦀".as_bytes()));
        assert_ne!(
            multi_hash(&values),
            multi_hash(&[Some("\x1e"), Some("\0naïve 🦀")])
        );
    }

    #[test]
    fn test_framing_is_deterministic() {
        let values = [Some("alpha"), None, Some("beta")];
        assert_eq!(
            encode_framed_components(&values),
            encode_framed_components(&values)
        );
        assert_eq!(multi_hash(&values), multi_hash(&values));
    }
}
