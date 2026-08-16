// TEST-4: cargo-fuzz target for the pg_trickle parser / rewrite pipeline.
//
// This target exercises all pure-Rust rewrite-pass entry points with
// arbitrary byte sequences. The goal is to ensure that no input can cause
// an abort, buffer overflow, or panic in the pre-parser validation layer.
//
// Functions under test (pure Rust, no backend required):
//   - validate_cron                 (api/helpers)
//   - parse_schedule                (api/helpers)
//   - detect_select_star            (api/helpers)
//   - detect_volatile_functions     (api/helpers — OP-6)
//   - expr_has_aggregate            (dvm/parser/rewrites — internal)
//   - contains_word_boundary        (dvm/parser/rewrites — internal)
//
// Run locally with:
//   cargo +nightly fuzz run api_guard_fuzz -- -max_total_time=60
//
// ⚠ The rewrite functions that call pg_parse_query() are NOT exercised here
// because they require a live PostgreSQL backend.  Integration-level fuzzing
// of those functions is left to libpq-based corpus drivers.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Require valid UTF-8 so we can pass &str to the functions under test.
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };

    // -------------------------------------------------------------------
    // 1. Schedule parsing — must never panic or abort.
    // -------------------------------------------------------------------
    let _ = pg_trickle::api::helpers::parse_schedule_pub(s);

    // -------------------------------------------------------------------
    // 2. Cron validation — must never panic or abort.
    // -------------------------------------------------------------------
    let _ = pg_trickle::api::helpers::validate_cron_pub(s);

    // -------------------------------------------------------------------
    // 3. SELECT * detection — must never panic or abort.
    // -------------------------------------------------------------------
    let _ = pg_trickle::api::helpers::detect_select_star_pub(s);

    // -------------------------------------------------------------------
    // 4. Volatile function detection (OP-6) — must never panic or abort.
    // -------------------------------------------------------------------
    let _ = pg_trickle::api::helpers::detect_volatile_functions_pub(s);
});
