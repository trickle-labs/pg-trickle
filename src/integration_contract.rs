//! Canonical, versioned encoding for integration contracts.
//!
//! The encoded form is deliberately independent of JSON serialization. Fields
//! retain caller-supplied order; set members and JSON object entries are sorted
//! by their encoded bytes.

use sha2::{Digest, Sha256};

/// Version of the Graph V1 typed contract encoding.
pub const CONTRACT_ENCODING_VERSION: u16 = 1;

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_I64: u8 = 2;
const TAG_U64: u8 = 3;
const TAG_TEXT: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_ARRAY: u8 = 6;
const TAG_SET: u8 = 7;

/// A value in the contract's canonical typed encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    Text(String),
    Bytes(Vec<u8>),
    /// Elements retain their order.
    Array(Vec<CanonicalValue>),
    /// Elements are sorted by encoded bytes before they are written.
    Set(Vec<CanonicalValue>),
}

/// A fixed-tagged contract field. The slice order is the normative field order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalField {
    pub tag: u16,
    pub value: CanonicalValue,
}

impl CanonicalField {
    pub const fn new(tag: u16, value: CanonicalValue) -> Self {
        Self { tag, value }
    }
}

/// Encode contract fields using the version, fixed tags, type tags, and
/// big-endian lengths required by the proposal.
pub fn encode_contract(fields: &[CanonicalField]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&CONTRACT_ENCODING_VERSION.to_be_bytes());
    for field in fields {
        output.extend_from_slice(&field.tag.to_be_bytes());
        encode_value(&mut output, &field.value);
    }
    output
}

/// Encode one typed value, without a field tag.
pub fn encode_value(output: &mut Vec<u8>, value: &CanonicalValue) {
    let (type_tag, payload) = match value {
        CanonicalValue::Null => (TAG_NULL, Vec::new()),
        CanonicalValue::Bool(value) => (TAG_BOOL, vec![u8::from(*value)]),
        CanonicalValue::I64(value) => (TAG_I64, value.to_be_bytes().to_vec()),
        CanonicalValue::U64(value) => (TAG_U64, value.to_be_bytes().to_vec()),
        CanonicalValue::Text(value) => (TAG_TEXT, value.as_bytes().to_vec()),
        CanonicalValue::Bytes(value) => (TAG_BYTES, value.clone()),
        CanonicalValue::Array(values) => encode_sequence(values, false),
        CanonicalValue::Set(values) => encode_sequence(values, true),
    };
    output.push(type_tag);
    output.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    output.extend_from_slice(&payload);
}

/// Return the SHA-256 digest of the canonical contract encoding.
pub fn contract_digest(fields: &[CanonicalField]) -> [u8; 32] {
    sha256_digest(&encode_contract(fields))
}

/// Return the canonical contract digest as lowercase hexadecimal.
pub fn contract_digest_hex(fields: &[CanonicalField]) -> String {
    sha256_hex(&encode_contract(fields))
}

/// Return the SHA-256 digest of arbitrary canonical bytes.
pub fn sha256_digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Return a SHA-256 digest as lowercase hexadecimal.
pub fn sha256_hex(bytes: &[u8]) -> String {
    sha256_digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Convert the proposal's JSON diagnostic values into typed values.
///
/// JSON objects become byte-sorted sets of text-key/value pairs. Floating
/// point numbers are rejected because JSON does not preserve their exact
/// representation across producers.
pub fn value_from_json(value: &serde_json::Value) -> Result<CanonicalValue, &'static str> {
    match value {
        serde_json::Value::Null => Ok(CanonicalValue::Null),
        serde_json::Value::Bool(value) => Ok(CanonicalValue::Bool(*value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(CanonicalValue::I64)
            .or_else(|| value.as_u64().map(CanonicalValue::U64))
            .ok_or("floating-point JSON values are not supported"),
        serde_json::Value::String(value) => Ok(CanonicalValue::Text(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>, _>>()
            .map(CanonicalValue::Array),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                Ok(CanonicalValue::Array(vec![
                    CanonicalValue::Text(key.clone()),
                    value_from_json(value)?,
                ]))
            })
            .collect::<Result<Vec<_>, &'static str>>()
            .map(CanonicalValue::Set),
    }
}

fn encode_sequence(values: &[CanonicalValue], sort: bool) -> (u8, Vec<u8>) {
    let mut encoded: Vec<Vec<u8>> = values
        .iter()
        .map(|value| {
            let mut bytes = Vec::new();
            encode_value(&mut bytes, value);
            bytes
        })
        .collect();
    if sort {
        encoded.sort();
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(&(encoded.len() as u64).to_be_bytes());
    for value in encoded {
        payload.extend_from_slice(&value);
    }
    (if sort { TAG_SET } else { TAG_ARRAY }, payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(tag: u16, value: CanonicalValue) -> CanonicalField {
        CanonicalField::new(tag, value)
    }

    #[test]
    fn encoding_and_digest_are_deterministic() {
        let fields = [field(7, CanonicalValue::Text("hello".into()))];
        assert_eq!(encode_contract(&fields), encode_contract(&fields));
        assert_eq!(contract_digest(&fields), contract_digest(&fields));
        assert_eq!(contract_digest_hex(&fields).len(), 64);
    }

    #[test]
    fn field_order_is_preserved_and_affects_digest() {
        let first = [
            field(1, CanonicalValue::U64(1)),
            field(2, CanonicalValue::U64(2)),
        ];
        let second = [
            field(2, CanonicalValue::U64(2)),
            field(1, CanonicalValue::U64(1)),
        ];
        assert_ne!(encode_contract(&first), encode_contract(&second));
        assert_ne!(contract_digest(&first), contract_digest(&second));
    }

    #[test]
    fn set_order_is_canonical_but_array_order_is_not() {
        let set_a = [field(
            1,
            CanonicalValue::Set(vec![
                CanonicalValue::Text("b".into()),
                CanonicalValue::Text("a".into()),
            ]),
        )];
        let set_b = [field(
            1,
            CanonicalValue::Set(vec![
                CanonicalValue::Text("a".into()),
                CanonicalValue::Text("b".into()),
            ]),
        )];
        assert_eq!(encode_contract(&set_a), encode_contract(&set_b));

        let array = |values| field(1, CanonicalValue::Array(values));
        assert_ne!(
            encode_contract(&[array(vec![
                CanonicalValue::Text("a".into()),
                CanonicalValue::Text("b".into())
            ])]),
            encode_contract(&[array(vec![
                CanonicalValue::Text("b".into()),
                CanonicalValue::Text("a".into())
            ])])
        );
    }
}
