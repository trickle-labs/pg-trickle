// FUZZ-9 (v0.80.0): P-5 — DVM snapshot fingerprint classifier stability fuzz.
//
// This target exercises the DVM query-pattern classifiers (which guard the
// snapshot fingerprint cache path) under arbitrary SQL string inputs to ensure:
//   1. No classifier panics or produces undefined behaviour.
//   2. Each classifier is deterministic (same input → same output).
//   3. The three classifiers are mutually exclusive for known-safe queries
//      (a query matching DVM-1 should not also match DVM-2).
//   4. Known-safe queries (simple scan, pure filter) never trigger any classifier.
//
// Functions under test (pure Rust, no PostgreSQL backend required):
//   - refresh_classify_case_in_list_pub        — DVM-1 / q12-like guard
//   - refresh_classify_correlated_subquery_pub — DVM-2 / q20-like guard
//   - refresh_classify_case_aggregate_subquery_uncertain_pub — O-1 / REGEX_COMPLEXITY guard
//
// Run locally:
//   cargo +nightly fuzz run snapshot_fingerprint_fuzz -- -max_total_time=60

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Require valid UTF-8; silently skip binary inputs.
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };

    // -------------------------------------------------------------------
    // 1. No classifier may panic.
    // -------------------------------------------------------------------
    let case_in_list = pg_trickle::fuzz_pub::refresh_classify_case_in_list_pub(s);
    let correlated = pg_trickle::fuzz_pub::refresh_classify_correlated_subquery_pub(s);
    let uncertain = pg_trickle::fuzz_pub::refresh_classify_case_aggregate_subquery_uncertain_pub(s);

    // -------------------------------------------------------------------
    // 2. All classifiers must be deterministic (same input → same output).
    // -------------------------------------------------------------------
    let case_in_list2 = pg_trickle::fuzz_pub::refresh_classify_case_in_list_pub(s);
    let correlated2 = pg_trickle::fuzz_pub::refresh_classify_correlated_subquery_pub(s);
    let uncertain2 = pg_trickle::fuzz_pub::refresh_classify_case_aggregate_subquery_uncertain_pub(s);

    assert_eq!(case_in_list, case_in_list2, "classify_case_in_list must be deterministic");
    assert_eq!(correlated, correlated2, "classify_correlated must be deterministic");
    assert_eq!(uncertain, uncertain2, "classify_uncertain must be deterministic");

    // -------------------------------------------------------------------
    // 3. Known-safe queries must never trigger any classifier.
    // -------------------------------------------------------------------
    let safe_queries: &[&str] = &[
        "SELECT id, amount FROM orders",
        "SELECT id, name FROM customers WHERE status = 'ACTIVE'",
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
    ];
    for safe in safe_queries {
        let c1 = pg_trickle::fuzz_pub::refresh_classify_case_in_list_pub(safe);
        let c2 = pg_trickle::fuzz_pub::refresh_classify_correlated_subquery_pub(safe);
        let c3 = pg_trickle::fuzz_pub::refresh_classify_case_aggregate_subquery_uncertain_pub(safe);
        assert!(!c1, "safe query triggered classify_case_in_list: {safe}");
        assert!(!c2, "safe query triggered classify_correlated: {safe}");
        assert!(!c3, "safe query triggered classify_uncertain: {safe}");
    }

    // -------------------------------------------------------------------
    // 4. A query matching DVM-1 (CASE_IN_LIST) should NOT also match DVM-2
    //    (correlated subquery) — they guard different patterns.
    //    This is a best-effort invariant check for known canonical forms.
    // -------------------------------------------------------------------
    let q12_canonical = "SELECT SUM(CASE WHEN col IN ('A','B') THEN 1 ELSE 0 END) FROM t";
    assert!(
        pg_trickle::fuzz_pub::refresh_classify_case_in_list_pub(q12_canonical),
        "q12 canonical must trigger classify_case_in_list"
    );
    assert!(
        !pg_trickle::fuzz_pub::refresh_classify_correlated_subquery_pub(q12_canonical),
        "q12 canonical must NOT trigger classify_correlated"
    );
});
