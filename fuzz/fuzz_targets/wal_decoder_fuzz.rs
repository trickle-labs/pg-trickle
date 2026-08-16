// Production test_decoding parser seam fuzz target.
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;

fuzz_target!(|data: &[u8]| {
    let Ok(input) = std::str::from_utf8(data) else { return };
    let _ = pg_trickle::wal_decoder::extract_test_decoding_table_for_fuzz(input);
    let _ = pg_trickle::wal_decoder::parse_test_decoding_action_for_fuzz(input);
    let columns = pg_trickle::wal_decoder::parse_test_decoding_columns_for_fuzz(input);
    let _old = pg_trickle::wal_decoder::parse_test_decoding_old_columns_for_fuzz(input);
    let expected: Vec<(String, String)> = columns
        .keys()
        .map(|name| (name.clone(), "text".to_string()))
        .collect();
    let _ = pg_trickle::wal_decoder::detect_test_decoding_schema_mismatch_for_fuzz(
        &columns, &expected,
    );
    let keys: Vec<String> = columns.keys().cloned().collect();
    let _ = pg_trickle::wal_decoder::build_test_decoding_parameter_plan_for_fuzz(
        &keys,
        &HashMap::from_iter(columns),
    );
});
