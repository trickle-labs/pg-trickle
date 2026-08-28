use pg_trickle::dvm::row_id_v2::{
    EncodedField, IdentityDomain, TYPE_INT4, TYPE_INTERVAL, TYPE_NUMERIC, TYPE_TEXT, encode_bytes,
    encode_interval_value, encode_numeric_text, encode_signed, encode_tuple,
};
use serde_json::Value;
use std::collections::HashSet;

const CORPUS: &str = include_str!("fixtures/row_id_v2_vectors.json");

fn hex(value: &Value) -> &str {
    value["bytes"]
        .as_str()
        .expect("vector bytes must be a string")
}

fn decode_hex(value: &str) -> Vec<u8> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty(), "hex must contain complete byte pairs");
    pairs
        .iter()
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16).expect("hex")
        })
        .collect()
}

#[test]
fn row_id_v2_corpus_is_independent_and_exact() {
    let corpus: Value = serde_json::from_str(CORPUS).expect("row-id V2 corpus must be JSON");
    assert_eq!(corpus["wire"]["identity_version"], 2);
    assert_eq!(corpus["wire"]["probe_version"], 1);
    assert_eq!(corpus["wire"]["probe_prefix_bytes"], 128);
    assert_eq!(corpus["wire"]["tuple_end"], "ff");
    assert_eq!(corpus["wire"]["variable_escape"], "00ff");
    assert_eq!(corpus["wire"]["variable_end"], "0000");

    let vectors = corpus["vectors"]
        .as_array()
        .expect("vectors must be an array");
    let names: HashSet<_> = vectors
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), vectors.len(), "vector names must be unique");
    assert!(vectors.len() >= 16);

    for vector in vectors {
        let bytes = hex(vector);
        assert!(!bytes.is_empty() && bytes.len().is_multiple_of(2));
        assert!(bytes.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(
            bytes.ends_with("ff"),
            "{} is not tuple-terminated",
            vector["name"]
        );
        let fields = vector["fields"]
            .as_array()
            .expect("fields must be an array");
        assert!(!fields.is_empty());
        assert_eq!(&bytes[0..2], "02");
        assert_eq!(
            &bytes[2..4],
            corpus["wire"]["domain_tags"][vector["domain"].as_str().unwrap()]
                .as_str()
                .unwrap()
        );
        assert_eq!(&bytes[4..12], format!("{:08x}", fields.len()));
    }

    assert_eq!(
        vectors
            .iter()
            .find(|v| v["name"] == "scan_int4_minus_one")
            .map(hex),
        Some("02010000000103017fffffff")
    );
    assert_eq!(
        vectors
            .iter()
            .find(|v| v["name"] == "group_float_positive_zero")
            .map(hex),
        vectors
            .iter()
            .find(|v| v["name"] == "group_float_negative_zero_canonical")
            .map(hex)
    );
    assert_eq!(
        vectors
            .iter()
            .find(|v| v["name"] == "group_float_nan_canonical")
            .map(hex),
        Some("0203000000010701fff8000000000000ff")
    );

    let payload = encode_signed(1, 4).expect("int4 payload");
    let generated = encode_tuple(
        IdentityDomain::ScanKey,
        &[EncodedField::value(TYPE_INT4, &payload)],
    )
    .expect("int4 identity");
    assert_eq!(generated, decode_hex("020100000001030180000001ff"));

    let payload = encode_bytes(b"a\0b");
    let generated = encode_tuple(
        IdentityDomain::ScanKey,
        &[EncodedField::value(TYPE_TEXT, &payload)],
    )
    .expect("text identity");
    assert_eq!(generated, decode_hex("02010000000109016100ff620000ff"));
    assert_eq!(
        encode_numeric_text("1.00").expect("numeric identity"),
        vec![0x03, 0x80, 0, 0, 0, 0, 0, 0, 1, b'1']
    );
    let numeric = encode_numeric_text("1.00").expect("numeric payload");
    let generated = encode_tuple(
        IdentityDomain::ScanKey,
        &[EncodedField::value(TYPE_NUMERIC, &numeric)],
    )
    .expect("numeric identity");
    assert_eq!(
        generated,
        decode_hex("020100000001080103800000000000000131ff")
    );
    let interval = encode_interval_value(0, 30, 0);
    let generated = encode_tuple(
        IdentityDomain::WindowKey,
        &[EncodedField::value(TYPE_INTERVAL, &interval)],
    )
    .expect("interval identity");
    assert_eq!(
        generated,
        decode_hex("020600000001130180000000000000000000025b7f3d4000ff")
    );

    for probe in corpus["probe_vectors"].as_array().unwrap() {
        let identity = probe["identity_hex"].as_str().unwrap();
        let encoded = probe["probe_hex"].as_str().unwrap();
        if identity.len() / 2 <= 128 {
            assert_eq!(identity, encoded);
        } else {
            assert_eq!(encoded.len() / 2, 144);
            assert!(encoded.starts_with(&identity[..256]));
        }
    }
    for pair in corpus["adversarial_pairs"].as_array().unwrap() {
        let left = vectors.iter().find(|v| v["name"] == pair["left"]).unwrap();
        let right = vectors.iter().find(|v| v["name"] == pair["right"]).unwrap();
        if pair["must_differ"].as_bool().unwrap() {
            assert_ne!(hex(left), hex(right));
        } else {
            assert_eq!(hex(left), hex(right));
        }
    }
}
