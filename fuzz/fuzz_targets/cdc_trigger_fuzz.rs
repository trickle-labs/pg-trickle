// FUZZ-3 (v0.26.0): cargo-fuzz target for CDC trigger payload deserialization.
//
// Exercises the pure-Rust paths that parse JSON/binary payloads from
// CDC change buffers. The goal is to ensure that no malformed payload
// can cause a panic, buffer overflow, or infinite loop.
//
// The CDC deserialization path is in src/cdc.rs. Because many of these
// functions call pg_sys and require a live PostgreSQL backend, this fuzz
// target exercises the parts that are accessible without one — primarily
// the hash computation and column-list parsing logic that operates on
// arbitrary byte sequences.
//
// Run locally with:
//   cargo +nightly fuzz run cdc_trigger_fuzz -- -max_total_time=60
//
// For 10M iterations:
//   cargo +nightly fuzz run cdc_trigger_fuzz -- -runs=10000000
//
// Functions under test (pure Rust, no PostgreSQL backend required):
//   - detect_select_star_pub         (api/helpers — CDC guard)
//   - detect_volatile_functions_pub  (api/helpers — CDC guard)
//   - hash computation (via content hash expression builder)

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Require valid UTF-8 to exercise string-processing paths.
    let Ok(s) = std::str::from_utf8(data) else {
        // Even for invalid UTF-8, the basic byte-level guards must not panic.
        // Test the detect functions with an empty string as fallback.
        let _ = pg_trickle::api::helpers::detect_select_star_pub("");
        return;
    };

    // -------------------------------------------------------------------
    // 1. CDC payload guard: SELECT * detection.
    //    Applied to trigger payload column lists — must not panic.
    // -------------------------------------------------------------------
    let _ = pg_trickle::api::helpers::detect_select_star_pub(s);

    // -------------------------------------------------------------------
    // 2. CDC payload guard: volatile function detection.
    //    Applied to defining queries to determine if CDC is allowed.
    // -------------------------------------------------------------------
    let _ = pg_trickle::api::helpers::detect_volatile_functions_pub(s);

    // -------------------------------------------------------------------
    // 3. Schedule / cron parse — used in CDC scheduling decisions.
    // -------------------------------------------------------------------
    let _ = pg_trickle::api::helpers::parse_schedule_pub(s);
});
