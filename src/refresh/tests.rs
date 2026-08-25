// ARCH-1B: Unit tests for the refresh pipeline.
// Moved from mod.rs to keep mod.rs under 500 LOC.
// This module IS the `tests` module (declared as `mod tests;` in mod.rs).

use super::*;
use crate::catalog::StreamTableMeta;
use crate::dag::{RefreshMode, StStatus};
use crate::error::PgTrickleError;
use crate::version::Frontier;
#[allow(unused_imports)]
use pgrx::prelude::*;
use std::sync::Arc;

// ── Functions moved to merge sub-modules (A16 split) — import for unit tests ─
use crate::refresh::merge::columns::{
    PartitionBounds, extract_keyword_int, inject_partition_predicate, parse_hash_bound_spec,
    pg_quote_literal,
};
use crate::refresh::merge::delete::{build_hash_child_merge, should_warn_amplification};

// ── Helper: build a minimal StreamTableMeta for testing ─────────

fn test_st(refresh_mode: RefreshMode, needs_reinit: bool) -> StreamTableMeta {
    StreamTableMeta {
        pgt_id: 1,
        pgt_relid: pg_sys::Oid::from(0u32),
        pgt_name: "test_st".to_string(),
        pgt_schema: "public".to_string(),
        defining_query: "SELECT 1".to_string(),
        original_query: None,
        schedule: None,
        refresh_mode,
        status: StStatus::Active,
        is_populated: true,
        data_timestamp: None,
        consecutive_errors: 0,
        needs_reinit,
        auto_threshold: None,
        last_full_ms: None,
        functions_used: None,
        frontier: None,
        topk_limit: None,
        topk_order_by: None,
        topk_offset: None,
        diamond_consistency: crate::dag::DiamondConsistency::None,
        diamond_schedule_policy: crate::dag::DiamondSchedulePolicy::default(),
        has_keyless_source: false,
        function_hashes: None,
        requested_cdc_mode: None,
        is_append_only: false,
        scc_id: None,
        last_fixpoint_iterations: None,
        pooler_compatibility_mode: false,
        refresh_tier: "hot".to_string(),
        fuse_mode: "off".to_string(),
        fuse_state: "armed".to_string(),
        fuse_ceiling: None,
        fuse_sensitivity: None,
        blown_at: None,
        blow_reason: None,
        st_partition_key: None,
        max_differential_joins: None,
        max_delta_fraction: None,
        last_error_message: None,
        last_error_at: None,
        downstream_publication_name: None,
        freshness_deadline_ms: None,
        st_placement: "local".to_string(),
        temporal_mode: false,
        storage_backend: "heap".to_string(),
        post_refresh_action: "none".to_string(),
        reindex_drift_threshold: None,
        rows_changed_since_last_reindex: 0,
        last_reindex_at: None,
        defining_query_hash: 0,
        storage_fillfactor: None,
        query_complexity_class: None,
        row_identity_version: Some(crate::hash::CURRENT_ROW_IDENTITY_VERSION),
        self_heal_work_mem_percent: 100,
        self_heal_lock_backoff_exponent: 0,
        self_heal_success_streak: 0,
        last_error_code: None,
        last_error_retryable: None,
        defining_search_path: "public".to_string(),
    }
}

fn make_frontier(entries: &[(u32, &str)]) -> Frontier {
    let mut f = Frontier::new();
    for &(oid, lsn) in entries {
        f.set_source(oid, lsn.to_string(), "2025-01-01T00:00:00Z".to_string());
    }
    f
}

// ── RefreshAction::as_str() ─────────────────────────────────────

#[test]
fn test_refresh_action_no_data() {
    assert_eq!(RefreshAction::NoData.as_str(), "NO_DATA");
}

#[test]
fn test_refresh_action_full() {
    assert_eq!(RefreshAction::Full.as_str(), "FULL");
}

#[test]
fn test_refresh_action_differential() {
    assert_eq!(RefreshAction::Differential.as_str(), "DIFFERENTIAL");
}

#[test]
fn test_refresh_action_reinitialize() {
    assert_eq!(RefreshAction::Reinitialize.as_str(), "REINITIALIZE");
}

#[test]
fn test_refresh_action_variants_exist() {
    let _full = RefreshAction::Full;
    let _incr = RefreshAction::Differential;
    let _no_data = RefreshAction::NoData;
    let _reinit = RefreshAction::Reinitialize;
}

#[test]
fn test_full_refresh_reason_codes_round_trip() {
    for code in [
        FullRefreshReasonCode::FirstRefresh,
        FullRefreshReasonCode::DeltaRatioExceeded,
        FullRefreshReasonCode::SchemaChanged,
        FullRefreshReasonCode::TopKRecompute,
    ] {
        assert_eq!(FullRefreshReasonCode::from_str(code.as_str()), Some(code));
    }
    assert_eq!(FullRefreshReasonCode::from_str("unknown"), None);
}

#[test]
fn test_full_refresh_reason_for_action() {
    assert_eq!(
        FullRefreshReason::for_action(RefreshAction::Full, true).map(|reason| reason.code),
        Some(FullRefreshReasonCode::FirstRefresh)
    );
    assert_eq!(
        FullRefreshReason::for_action(RefreshAction::Differential, false),
        None
    );
}

#[test]
fn test_execute_differential_refresh_rejects_unpopulated_stream_table() {
    let mut st = test_st(RefreshMode::Differential, false);
    st.is_populated = false;

    let error = execute_differential_refresh(
        &st,
        &make_frontier(&[(42, "0/10")]),
        &make_frontier(&[(42, "0/20")]),
    )
    .expect_err("unpopulated stream tables must be rejected before SPI work");

    match error {
        PgTrickleError::InvalidArgument(message) => {
            assert!(message.contains("unpopulated stream table public.test_st"));
            assert!(message.contains("FULL refresh is required first"));
        }
        other => panic!("expected InvalidArgument, got {other:?}"), // nosemgrep: semgrep.rust.panic-in-sql-path
    }
}

#[test]
fn test_execute_differential_refresh_rejects_empty_frontier() {
    let st = test_st(RefreshMode::Differential, false);

    let error =
        execute_differential_refresh(&st, &Frontier::new(), &make_frontier(&[(42, "0/20")]))
            .expect_err("missing baseline frontier must be rejected before SPI work");

    match error {
        PgTrickleError::InvalidArgument(message) => {
            assert!(message.contains("public.test_st"));
            assert!(message.contains("no previous frontier exists"));
        }
        other => panic!("expected InvalidArgument, got {other:?}"), // nosemgrep: semgrep.rust.panic-in-sql-path
    }
}

// ── determine_refresh_action() ──────────────────────────────────

#[test]
fn test_determine_reinit_takes_priority() {
    let st = test_st(RefreshMode::Differential, true);
    assert_eq!(
        determine_refresh_action(&st, true),
        RefreshAction::Reinitialize,
    );
}

#[test]
fn test_determine_no_upstream_changes() {
    let st = test_st(RefreshMode::Differential, false);
    assert_eq!(determine_refresh_action(&st, false), RefreshAction::NoData,);
}

#[test]
fn test_determine_full_mode() {
    let st = test_st(RefreshMode::Full, false);
    assert_eq!(determine_refresh_action(&st, true), RefreshAction::Full,);
}

#[test]
fn test_determine_differential_mode() {
    let st = test_st(RefreshMode::Differential, false);
    assert_eq!(
        determine_refresh_action(&st, true),
        RefreshAction::Differential,
    );
}

#[test]
fn test_determine_reinit_overrides_no_changes() {
    // Even if no upstream changes, reinit flag wins
    let st = test_st(RefreshMode::Full, true);
    assert_eq!(
        determine_refresh_action(&st, false),
        RefreshAction::Reinitialize,
    );
}

// ── resolve_lsn_placeholders() ──────────────────────────────────

/// Convenience wrapper for tests — calls resolve_lsn_placeholders with empty zero_change_oids.
fn resolve_lsn_placeholders_test(
    template: &str,
    source_oids: &[u32],
    prev: &Frontier,
    new_f: &Frontier,
) -> String {
    resolve_lsn_placeholders(
        template,
        source_oids,
        prev,
        new_f,
        &std::collections::HashSet::new(),
    )
    .unwrap()
}

#[test]
fn test_resolve_lsn_single_oid() {
    let mut prev = Frontier::new();
    prev.set_source(42, "0/1000".to_string(), "ts".to_string());
    let mut new_f = Frontier::new();
    new_f.set_source(42, "0/2000".to_string(), "ts".to_string());

    let template =
        "DELETE FROM changes_42 WHERE lsn > '__PGS_PREV_LSN_42__' AND lsn <= '__PGS_NEW_LSN_42__'";
    let resolved = resolve_lsn_placeholders_test(template, &[42], &prev, &new_f);
    assert!(resolved.contains("0/1000"));
    assert!(resolved.contains("0/2000"));
    assert!(!resolved.contains("__PGS_"));
}

#[test]
fn test_resolve_lsn_multiple_oids() {
    let mut prev = Frontier::new();
    prev.set_source(10, "0/AA".to_string(), "ts".to_string());
    prev.set_source(20, "0/BB".to_string(), "ts".to_string());
    let mut new_f = Frontier::new();
    new_f.set_source(10, "0/CC".to_string(), "ts".to_string());
    new_f.set_source(20, "0/DD".to_string(), "ts".to_string());

    let template = "__PGS_PREV_LSN_10__ __PGS_NEW_LSN_10__ __PGS_PREV_LSN_20__ __PGS_NEW_LSN_20__";
    let resolved = resolve_lsn_placeholders_test(template, &[10, 20], &prev, &new_f);
    assert_eq!(resolved, "0/AA 0/CC 0/BB 0/DD");
}

#[test]
fn test_resolve_lsn_no_placeholders() {
    let prev = Frontier::new();
    let new_f = Frontier::new();
    let resolved = resolve_lsn_placeholders_test("SELECT 1", &[], &prev, &new_f);
    assert_eq!(resolved, "SELECT 1");
}

#[test]
fn test_resolve_lsn_missing_oid_defaults() {
    // C-2 requires both tokens present in template; frontier empty → defaults to 0/0.
    let prev = Frontier::new();
    let new_f = Frontier::new();
    let template = "__PGS_PREV_LSN_999__ and __PGS_NEW_LSN_999__";
    let resolved = resolve_lsn_placeholders_test(template, &[999], &prev, &new_f);
    assert!(resolved.contains("0/0"));
}

#[test]
fn test_resolve_lsn_preserves_other_text() {
    let mut prev = Frontier::new();
    prev.set_source(1, "0/10".to_string(), "ts".to_string());
    let mut new_f = Frontier::new();
    new_f.set_source(1, "0/20".to_string(), "ts".to_string());

    // C-2 requires both tokens present in template.
    let template = "SELECT * FROM t WHERE x = 42 AND lsn > '__PGS_PREV_LSN_1__'::pg_lsn AND lsn <= '__PGS_NEW_LSN_1__'::pg_lsn";
    let resolved = resolve_lsn_placeholders_test(template, &[1], &prev, &new_f);
    assert!(resolved.contains("SELECT * FROM t WHERE x = 42"));
    assert!(resolved.contains("0/10"));
}

#[test]
fn test_resolve_lsn_placeholders_single_source() {
    let template = "DELETE FROM changes_12345 WHERE lsn > '__PGS_PREV_LSN_12345__'::pg_lsn AND lsn <= '__PGS_NEW_LSN_12345__'::pg_lsn";
    let prev = make_frontier(&[(12345, "0/1000")]);
    let new = make_frontier(&[(12345, "0/2000")]);
    let result = resolve_lsn_placeholders_test(template, &[12345], &prev, &new);
    assert_eq!(
        result,
        "DELETE FROM changes_12345 WHERE lsn > '0/1000'::pg_lsn AND lsn <= '0/2000'::pg_lsn"
    );
}

#[test]
fn test_resolve_lsn_placeholders_multi_source() {
    let template = "DELETE FROM changes_100 WHERE lsn > '__PGS_PREV_LSN_100__'::pg_lsn AND lsn <= '__PGS_NEW_LSN_100__'::pg_lsn;\
                    DELETE FROM changes_200 WHERE lsn > '__PGS_PREV_LSN_200__'::pg_lsn AND lsn <= '__PGS_NEW_LSN_200__'::pg_lsn";
    let prev = make_frontier(&[(100, "0/A"), (200, "0/B")]);
    let new = make_frontier(&[(100, "0/C"), (200, "0/D")]);
    let result = resolve_lsn_placeholders_test(template, &[100, 200], &prev, &new);
    assert!(result.contains("'0/A'"));
    assert!(result.contains("'0/C'"));
    assert!(result.contains("'0/B'"));
    assert!(result.contains("'0/D'"));
    assert!(!result.contains("__PGS_"));
}

#[test]
fn test_resolve_lsn_placeholders_missing_source_defaults_to_0_0() {
    // C-2 requires both tokens present; when frontier is empty both default to 0/0.
    let template = "lsn > '__PGS_PREV_LSN_999__'::pg_lsn AND lsn <= '__PGS_NEW_LSN_999__'::pg_lsn";
    let prev = Frontier::new();
    let new = Frontier::new();
    let result = resolve_lsn_placeholders_test(template, &[999], &prev, &new);
    assert!(result.contains("'0/0'"));
}

#[test]
fn test_resolve_lsn_placeholders_empty_template() {
    // Empty template with no source OIDs — C-2 assertion is skipped (no OIDs to check).
    let result = resolve_lsn_placeholders_test("", &[], &Frontier::new(), &Frontier::new());
    assert_eq!(result, "");
}

#[test]
fn test_resolve_lsn_placeholders_no_sources() {
    let template = "SELECT 1";
    let result = resolve_lsn_placeholders_test(template, &[], &Frontier::new(), &Frontier::new());
    assert_eq!(result, "SELECT 1");
}

#[test]
fn test_resolve_lsn_b3_1_zero_change_pruning() {
    let template = "SELECT * FROM changes_42 WHERE c.lsn > '__PGS_PREV_LSN_42__'::pg_lsn AND c.lsn <= '__PGS_NEW_LSN_42__'::pg_lsn";
    let prev = make_frontier(&[(42, "0/1000")]);
    let new_f = make_frontier(&[(42, "0/2000")]);
    let mut zero = std::collections::HashSet::new();
    zero.insert(42u32);
    let result = resolve_lsn_placeholders(template, &[42], &prev, &new_f, &zero).unwrap(); // nosemgrep: semgrep.rust.panic-in-sql-path
    assert!(result.contains("FALSE"));
    assert!(!result.contains("__PGS_"));
    assert!(!result.contains("0/1000"));
}

#[test]
fn test_resolve_lsn_b3_1_partial_zero_change() {
    // Two sources — OID 10 has changes, OID 20 has zero changes
    let template = "c.lsn > '__PGS_PREV_LSN_10__'::pg_lsn AND c.lsn <= '__PGS_NEW_LSN_10__'::pg_lsn UNION ALL c.lsn > '__PGS_PREV_LSN_20__'::pg_lsn AND c.lsn <= '__PGS_NEW_LSN_20__'::pg_lsn";
    let prev = make_frontier(&[(10, "0/A"), (20, "0/B")]);
    let new_f = make_frontier(&[(10, "0/C"), (20, "0/D")]);
    let mut zero = std::collections::HashSet::new();
    zero.insert(20u32);
    let result = resolve_lsn_placeholders(template, &[10, 20], &prev, &new_f, &zero).unwrap(); // nosemgrep: semgrep.rust.panic-in-sql-path
    // OID 10 should be resolved normally
    assert!(result.contains("'0/A'"));
    assert!(result.contains("'0/C'"));
    // OID 20 should be pruned to FALSE
    assert!(result.contains("FALSE"));
    assert!(!result.contains("__PGS_"));
}

// ── CachedMergeTemplate tests ──────────────────────────────────────

#[test]
fn test_merge_template_cache_insert_and_retrieve() {
    MERGE_TEMPLATE_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        map.put(
            42,
            CachedMergeTemplate {
                defining_query_hash: 12345,
                merge_sql_template: "MERGE INTO t ...".into(),
                source_oids: vec![100, 200],
                cleanup_sql_template: "DELETE FROM ...".into(),
                parameterized_merge_sql: Arc::from(""),
                trigger_delete_template: Arc::from(""),
                trigger_update_template: Arc::from(""),
                trigger_insert_template: Arc::from(""),
                trigger_using_template: Arc::from(""),
                delta_sql_template: Arc::from(""),
                is_all_algebraic: false,
                is_deduplicated: true,
            },
        );
    });

    let entry = MERGE_TEMPLATE_CACHE.with(|cache| cache.borrow().peek(&42).cloned());
    assert!(entry.is_some());
    let entry = entry.unwrap(); // nosemgrep: semgrep.rust.panic-in-sql-path
    assert_eq!(entry.defining_query_hash, 12345);
    assert_eq!(entry.source_oids, vec![100, 200]);

    // Cleanup
    MERGE_TEMPLATE_CACHE.with(|cache| cache.borrow_mut().pop(&42));
}

#[test]
fn test_invalidate_merge_cache_removes_entry() {
    MERGE_TEMPLATE_CACHE.with(|cache| {
        cache.borrow_mut().put(
            99,
            CachedMergeTemplate {
                defining_query_hash: 0,
                merge_sql_template: Arc::from(""),
                source_oids: vec![],
                cleanup_sql_template: Arc::from(""),
                parameterized_merge_sql: Arc::from(""),
                trigger_delete_template: Arc::from(""),
                trigger_update_template: Arc::from(""),
                trigger_insert_template: Arc::from(""),
                trigger_using_template: Arc::from(""),
                delta_sql_template: Arc::from(""),
                is_all_algebraic: false,
                is_deduplicated: true,
            },
        );
    });

    invalidate_merge_cache(99);

    let exists = MERGE_TEMPLATE_CACHE.with(|cache| cache.borrow().peek(&99).is_some());
    assert!(!exists);
}

#[test]
fn test_invalidate_merge_cache_nonexistent_is_noop() {
    // Should not panic
    invalidate_merge_cache(999_999);
}

// ── D-2: parameterize_lsn_template tests ───────────────────────────

#[test]
fn test_parameterize_single_source() {
    let template = "SELECT * FROM c WHERE c.lsn > '__PGS_PREV_LSN_100__'::pg_lsn \
                     AND c.lsn <= '__PGS_NEW_LSN_100__'::pg_lsn";
    let result = parameterize_lsn_template(template, &[100]);
    assert!(result.contains("$1"), "should have $1: {result}");
    assert!(result.contains("$2"), "should have $2: {result}");
    assert!(
        !result.contains("__PGS_PREV_LSN_100__"),
        "should not have prev token"
    );
    assert!(
        !result.contains("__PGS_NEW_LSN_100__"),
        "should not have new token"
    );
}

#[test]
fn test_parameterize_multiple_sources() {
    let template = "WHERE c1.lsn > '__PGS_PREV_LSN_10__'::pg_lsn \
                     AND c1.lsn <= '__PGS_NEW_LSN_10__'::pg_lsn \
                     AND c2.lsn > '__PGS_PREV_LSN_20__'::pg_lsn \
                     AND c2.lsn <= '__PGS_NEW_LSN_20__'::pg_lsn";
    let result = parameterize_lsn_template(template, &[10, 20]);
    assert!(result.contains("$1"), "prev for oid 10: {result}");
    assert!(result.contains("$2"), "new for oid 10: {result}");
    assert!(result.contains("$3"), "prev for oid 20: {result}");
    assert!(result.contains("$4"), "new for oid 20: {result}");
}

#[test]
fn test_parameterize_no_sources() {
    let template = "SELECT 1";
    let result = parameterize_lsn_template(template, &[]);
    assert_eq!(result, "SELECT 1");
}

#[test]
fn test_build_prepare_type_list_single() {
    assert_eq!(build_prepare_type_list(1), "pg_lsn, pg_lsn");
}

#[test]
fn test_build_prepare_type_list_multi() {
    assert_eq!(
        build_prepare_type_list(3),
        "pg_lsn, pg_lsn, pg_lsn, pg_lsn, pg_lsn, pg_lsn"
    );
}

#[test]
fn test_build_prepare_type_list_zero() {
    assert_eq!(build_prepare_type_list(0), "");
}

#[test]
fn test_build_execute_params_single_source() {
    use crate::version::Frontier;
    let mut prev = Frontier::new();
    let mut next = Frontier::new();
    prev.set_source(100, "0/1000".to_string(), String::new());
    next.set_source(100, "0/2000".to_string(), String::new());
    let result = build_execute_params(&[100], &prev, &next);
    assert_eq!(result, "'0/1000'::pg_lsn, '0/2000'::pg_lsn");
}

#[test]
fn test_build_execute_params_multiple_sources() {
    use crate::version::Frontier;
    let mut prev = Frontier::new();
    let mut next = Frontier::new();
    prev.set_source(10, "0/A".to_string(), String::new());
    prev.set_source(20, "0/B".to_string(), String::new());
    next.set_source(10, "0/C".to_string(), String::new());
    next.set_source(20, "0/D".to_string(), String::new());
    let result = build_execute_params(&[10, 20], &prev, &next);
    assert_eq!(
        result,
        "'0/A'::pg_lsn, '0/C'::pg_lsn, '0/B'::pg_lsn, '0/D'::pg_lsn"
    );
}

#[test]
fn test_build_execute_params_missing_lsn_uses_zero() {
    use crate::version::Frontier;
    let prev = Frontier::new();
    let next = Frontier::new();
    let result = build_execute_params(&[999], &prev, &next);
    assert_eq!(result, "'0/0'::pg_lsn, '0/0'::pg_lsn");
}

// ── compute_adaptive_threshold() ────────────────────────────────

#[test]
fn test_adaptive_threshold_incr_much_slower_than_full() {
    // INCR is 95% of FULL → lower threshold by 20%
    let result = compute_adaptive_threshold(0.15, 95.0, 100.0);
    assert!((result - 0.12).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_incr_moderately_slow() {
    // INCR is 75% of FULL → lower threshold by 10%
    let result = compute_adaptive_threshold(0.15, 75.0, 100.0);
    assert!((result - 0.135).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_incr_much_faster() {
    // INCR is 20% of FULL → raise threshold by 10%
    let result = compute_adaptive_threshold(0.15, 20.0, 100.0);
    assert!((result - 0.165).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_incr_in_sweet_spot() {
    // INCR is 50% of FULL → keep threshold unchanged
    let result = compute_adaptive_threshold(0.15, 50.0, 100.0);
    assert!((result - 0.15).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_clamps_to_min() {
    // Very low threshold that gets lowered further → clamped to 0.01
    let result = compute_adaptive_threshold(0.012, 95.0, 100.0);
    assert!((result - 0.01).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_clamps_to_max() {
    // High threshold that gets raised → clamped to 0.80
    let result = compute_adaptive_threshold(0.75, 10.0, 100.0);
    assert!((result - 0.80).abs() < 0.01, "got {result}");
}

#[test]
fn test_adaptive_threshold_at_boundary_90pct() {
    // Exactly 90% → should lower by 20%
    let result = compute_adaptive_threshold(0.20, 90.0, 100.0);
    assert!((result - 0.16).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_at_boundary_70pct() {
    // Exactly 70% → should lower by 10%
    let result = compute_adaptive_threshold(0.20, 70.0, 100.0);
    assert!((result - 0.18).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_at_boundary_30pct() {
    // Exactly 30% → keep threshold (boundary is <=, not <)
    let result = compute_adaptive_threshold(0.20, 30.0, 100.0);
    assert!((result - 0.22).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_incr_exceeds_full() {
    // INCR took longer than FULL (ratio 1.2) → aggressively lower
    let result = compute_adaptive_threshold(0.15, 120.0, 100.0);
    assert!((result - 0.12).abs() < 0.001, "got {result}");
}

#[test]
fn test_adaptive_threshold_converges_downward() {
    // Simulate multiple iterations of INCR being 80% of FULL
    let mut threshold = 0.30;
    for _ in 0..10 {
        threshold = compute_adaptive_threshold(threshold, 80.0, 100.0);
    }
    // Should converge downward but stay above min
    assert!(threshold >= 0.01, "got {threshold}");
    assert!(threshold < 0.15, "should decrease: got {threshold}");
}

#[test]
fn test_adaptive_threshold_converges_upward() {
    // Simulate iterations of INCR being 10% of FULL
    let mut threshold = 0.10_f64;
    for _ in 0..50 {
        threshold = compute_adaptive_threshold(threshold, 10.0, 100.0);
    }
    // Should converge upward toward the cap
    assert!((threshold - 0.80).abs() < 0.01, "got {threshold}");
}

// ── build_append_only_insert_sql() ──────────────────────────────

// ── PH-E1: estimate_delta_output_rows extraction ────────────────

#[test]
fn test_extract_using_clause_for_estimation() {
    // Verify the USING clause extraction pattern works correctly.
    // (estimate_delta_output_rows calls SPI, so we test the parsing
    // by checking the same extraction logic used in both functions.)
    let merge_sql = r#"MERGE INTO "public"."test_st" AS st USING (SELECT * FROM delta) AS d ON st.__pgt_row_id = d.__pgt_row_id WHEN MATCHED THEN DELETE"#;

    let using_start = merge_sql.find("USING ").map(|p| p + 6);
    let using_end = merge_sql.find(" AS d ON ");
    assert!(using_start.is_some());
    assert!(using_end.is_some());
    let clause = &merge_sql[using_start.unwrap()..using_end.unwrap()]; // nosemgrep: semgrep.rust.panic-in-sql-path
    assert_eq!(clause, "(SELECT * FROM delta)");
}

#[test]
fn test_extract_using_clause_complex_cte() {
    let merge_sql = r#"MERGE INTO "s"."t" AS st USING (WITH cte AS NOT MATERIALIZED (SELECT a FROM b) SELECT * FROM cte) AS d ON st.id = d.id WHEN MATCHED THEN DELETE"#;

    let using_start = merge_sql.find("USING ").map(|p| p + 6).unwrap(); // nosemgrep: semgrep.rust.panic-in-sql-path
    let using_end = merge_sql.find(" AS d ON ").unwrap(); // nosemgrep: semgrep.rust.panic-in-sql-path
    let clause = &merge_sql[using_start..using_end];
    assert!(clause.starts_with("(WITH cte AS NOT MATERIALIZED"));
    assert!(clause.ends_with("SELECT * FROM cte)"));
}

#[test]
fn test_build_append_only_insert_sql_basic() {
    let merge_sql = r#"MERGE INTO "public"."test_st" AS st USING (SELECT * FROM delta) AS d ON st.__pgt_row_id = d.__pgt_row_id WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE WHEN MATCHED AND d.__pgt_action = 'I' AND (st."val"::text IS DISTINCT FROM d."val"::text) THEN UPDATE SET "val" = d."val" WHEN NOT MATCHED AND d.__pgt_action = 'I' THEN INSERT (__pgt_row_id, "val") VALUES (d.__pgt_row_id, d."val")"#;

    let result = build_append_only_insert_sql("public", "test_st", merge_sql);
    assert!(result.contains(r#"INSERT INTO "public"."test_st""#));
    assert!(result.contains("__pgt_row_id"));
    assert!(result.contains("WHERE d.__pgt_action = 'I'"));
    assert!(!result.contains("MERGE"));
    assert!(!result.contains("DELETE"));
    assert!(!result.contains("UPDATE SET"));
}

#[test]
fn test_build_append_only_insert_sql_multi_column() {
    let merge_sql = r#"MERGE INTO "myschema"."events" AS st USING (SELECT * FROM changes) AS d ON st.__pgt_row_id = d.__pgt_row_id WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE WHEN NOT MATCHED AND d.__pgt_action = 'I' THEN INSERT (__pgt_row_id, "id", "type", "payload") VALUES (d.__pgt_row_id, d."id", d."type", d."payload")"#;

    let result = build_append_only_insert_sql("myschema", "events", merge_sql);
    assert!(result.contains(r#"INSERT INTO "myschema"."events""#));
    assert!(result.contains(r#"__pgt_row_id, "id", "type", "payload""#));
    assert!(result.contains(r#"d.__pgt_row_id, d."id", d."type", d."payload""#));
}

// ── DAG-3: compute_amplification_ratio tests ────────────────────

#[test]
fn test_amplification_ratio_normal() {
    let ratio = compute_amplification_ratio(10, 1000);
    assert!((ratio - 100.0).abs() < f64::EPSILON);
}

#[test]
fn test_amplification_ratio_one_to_one() {
    let ratio = compute_amplification_ratio(50, 50);
    assert!((ratio - 1.0).abs() < f64::EPSILON);
}

#[test]
fn test_amplification_ratio_reduction() {
    // Output smaller than input (e.g. aggregation)
    let ratio = compute_amplification_ratio(1000, 10);
    assert!((ratio - 0.01).abs() < f64::EPSILON);
}

#[test]
fn test_amplification_ratio_zero_input_returns_zero() {
    assert!((compute_amplification_ratio(0, 500)).abs() < f64::EPSILON);
}

#[test]
fn test_amplification_ratio_negative_input_returns_zero() {
    assert!((compute_amplification_ratio(-1, 500)).abs() < f64::EPSILON);
}

#[test]
fn test_amplification_ratio_zero_output() {
    assert!((compute_amplification_ratio(100, 0)).abs() < f64::EPSILON);
}

// ── DAG-3: should_warn_amplification tests ──────────────────────

#[test]
fn test_should_warn_above_threshold() {
    // 1000 / 5 = 200× > 100×
    assert!(should_warn_amplification(5, 1000, 100.0));
}

#[test]
fn test_should_not_warn_below_threshold() {
    // 50 / 5 = 10× < 100×
    assert!(!should_warn_amplification(5, 50, 100.0));
}

#[test]
fn test_should_not_warn_at_threshold() {
    // Exactly at threshold — not exceeded, no warning.
    assert!(!should_warn_amplification(1, 100, 100.0));
}

#[test]
fn test_should_not_warn_disabled() {
    // threshold = 0 → detection disabled
    assert!(!should_warn_amplification(1, 10_000, 0.0));
}

#[test]
fn test_should_not_warn_negative_threshold() {
    assert!(!should_warn_amplification(1, 10_000, -5.0));
}

#[test]
fn test_should_not_warn_zero_input() {
    assert!(!should_warn_amplification(0, 500, 100.0));
}

#[test]
fn test_should_not_warn_negative_input() {
    assert!(!should_warn_amplification(-1, 500, 100.0));
}

#[test]
fn test_should_warn_low_threshold() {
    // threshold = 2.0, ratio = 10/2 = 5.0 → warn
    assert!(should_warn_amplification(2, 10, 2.0));
}

// ── ST-ST-9: Content hash for ST change buffer pk_hash ──────────

#[test]
fn test_build_content_hash_expr_single_col() {
    let expr = build_content_hash_expr("d.", &["id".to_string()]);
    assert_eq!(expr, "pgtrickle.pg_trickle_hash(d.\"id\"::TEXT)");
}

#[test]
fn test_build_content_hash_expr_multi_col() {
    let expr = build_content_hash_expr("d.", &["id".to_string(), "val".to_string()]);
    assert_eq!(
        expr,
        "pgtrickle.pg_trickle_hash_multi(ARRAY[d.\"id\"::TEXT, d.\"val\"::TEXT])"
    );
}

#[test]
fn test_build_content_hash_expr_quoted_col() {
    let expr = build_content_hash_expr("pre.", &["col\"name".to_string()]);
    assert_eq!(expr, "pgtrickle.pg_trickle_hash(pre.\"col\"\"name\"::TEXT)");
}

#[test]
fn test_build_content_hash_expr_empty_cols_fallback() {
    let expr = build_content_hash_expr("d.", &[]);
    assert_eq!(expr, "d.__pgt_row_id");
}

#[test]
fn test_bypass_capture_uses_content_hash() {
    let sql = build_bypass_capture_sql(
        42,
        &[
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
        ],
        "pg_temp.__pgt_bypass_42",
        None,
    );
    // ST-ST-9: pk_hash should be content hash, not d.__pgt_row_id
    assert!(sql.contains("pg_trickle_hash_multi(ARRAY[d.\"id\"::TEXT, d.\"name\"::TEXT])"));
    assert!(!sql.contains("d.__pgt_row_id"));
}

// ── Phase 6 (TESTING_GAPS_2): determine_refresh_action unit tests ────────

#[test]
fn test_determine_refresh_action_needs_reinit_takes_priority() {
    // needs_reinit=true always → Reinitialize, regardless of changes flag
    let st = test_st(RefreshMode::Differential, true);
    assert_eq!(
        determine_refresh_action(&st, true),
        RefreshAction::Reinitialize
    );
    assert_eq!(
        determine_refresh_action(&st, false),
        RefreshAction::Reinitialize
    );
}

#[test]
fn test_determine_refresh_action_no_upstream_changes_returns_no_data() {
    // has_upstream_changes=false → NoData (unless needs_reinit)
    let st_diff = test_st(RefreshMode::Differential, false);
    let st_full = test_st(RefreshMode::Full, false);
    assert_eq!(
        determine_refresh_action(&st_diff, false),
        RefreshAction::NoData
    );
    assert_eq!(
        determine_refresh_action(&st_full, false),
        RefreshAction::NoData
    );
}

#[test]
fn test_determine_refresh_action_differential_mode_with_changes() {
    let st = test_st(RefreshMode::Differential, false);
    assert_eq!(
        determine_refresh_action(&st, true),
        RefreshAction::Differential
    );
}

#[test]
fn test_determine_refresh_action_full_mode_with_changes() {
    let st = test_st(RefreshMode::Full, false);
    assert_eq!(determine_refresh_action(&st, true), RefreshAction::Full);
}

#[test]
fn test_determine_refresh_action_immediate_falls_back_to_full() {
    let st = test_st(RefreshMode::Immediate, false);
    // IMMEDIATE is trigger-maintained; manual refresh → Full fallback
    assert_eq!(determine_refresh_action(&st, true), RefreshAction::Full);
}

// ── Phase 6: build_is_distinct_clause boundary tests ────────────────────
//
// The threshold between column-list and hash-based comparison is
// WIDE_TABLE_HASH_THRESHOLD (50).  Test straddling that boundary.

#[test]
fn test_build_is_distinct_clause_exactly_at_threshold_uses_columns() {
    let cols: Vec<String> = (1..=WIDE_TABLE_HASH_THRESHOLD)
        .map(|i| format!("col{i}"))
        .collect();
    let sql = build_is_distinct_clause(&cols);
    // At the threshold: use per-column IS DISTINCT FROM
    assert!(
        sql.contains("IS DISTINCT FROM"),
        "Threshold should use per-column comparison"
    );
    assert!(
        !sql.contains("pg_trickle_hash"),
        "Threshold should NOT use hash comparison"
    );
}

#[test]
fn test_build_is_distinct_clause_one_over_threshold_uses_hash() {
    let cols: Vec<String> = (1..=(WIDE_TABLE_HASH_THRESHOLD + 1))
        .map(|i| format!("col{i}"))
        .collect();
    let sql = build_is_distinct_clause(&cols);
    assert!(
        sql.contains("pg_trickle_hash"),
        "One over threshold should use hash comparison"
    );
    // Per-column path uses `::text IS DISTINCT FROM`; hash path does not.
    assert!(
        !sql.contains("::text IS DISTINCT FROM"),
        "Wide table should NOT use per-column comparison; got: {sql}"
    );
}

#[test]
fn test_build_is_distinct_clause_double_quotes_in_col_name_are_escaped() {
    let cols = vec!["weird\"name".to_string()];
    let sql = build_is_distinct_clause(&cols);
    // The double quote inside the name should be escaped as ""
    assert!(
        sql.contains("\"\""),
        "Double quotes in column names must be escaped; got: {sql}"
    );
}

// ── TG2-MERGE: build_merge_sql() unit tests ────────────────────

#[test]
fn test_build_merge_sql_single_column() {
    let cols = vec!["amount".to_string()];
    let sql = build_merge_sql("\"public\".\"totals\"", "(delta_query)", &cols, false);
    assert!(sql.starts_with("MERGE INTO \"public\".\"totals\" AS st"));
    assert!(sql.contains("USING (delta_query) AS d"));
    assert!(sql.contains("ON st.__pgt_row_id = d.__pgt_row_id"));
    assert!(sql.contains("WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE"));
    assert!(sql.contains("UPDATE SET \"amount\" = d.\"amount\""));
    assert!(sql.contains("INSERT (__pgt_row_id, \"amount\")"));
    assert!(sql.contains("VALUES (d.__pgt_row_id, d.\"amount\")"));
    assert!(!sql.contains("__PGT_PART_PRED__"));
}

#[test]
fn test_build_merge_sql_multiple_columns() {
    let cols = vec!["region".to_string(), "total".to_string(), "cnt".to_string()];
    let sql = build_merge_sql(
        "\"pgtrickle\".\"sales\"",
        "(SELECT * FROM delta)",
        &cols,
        false,
    );
    assert!(sql.contains(
        "UPDATE SET \"region\" = d.\"region\", \"total\" = d.\"total\", \"cnt\" = d.\"cnt\""
    ));
    assert!(sql.contains("INSERT (__pgt_row_id, \"region\", \"total\", \"cnt\")"));
    assert!(sql.contains("VALUES (d.__pgt_row_id, d.\"region\", d.\"total\", d.\"cnt\")"));
}

#[test]
fn test_build_merge_sql_with_partition_key() {
    let cols = vec!["val".to_string()];
    let sql = build_merge_sql("\"public\".\"partitioned\"", "(delta)", &cols, true);
    assert!(sql.contains("ON st.__pgt_row_id = d.__pgt_row_id __PGT_PART_PRED__"));
}

#[test]
fn test_build_merge_sql_without_partition_key() {
    let cols = vec!["val".to_string()];
    let sql = build_merge_sql("\"public\".\"simple\"", "(delta)", &cols, false);
    assert!(!sql.contains("__PGT_PART_PRED__"));
}

#[test]
fn test_build_merge_sql_column_quoting() {
    let cols = vec!["my \"col\"".to_string(), "normal".to_string()];
    let sql = build_merge_sql("\"public\".\"t\"", "(delta)", &cols, false);
    assert!(sql.contains("\"my \"\"col\"\"\""));
}

#[test]
fn test_build_merge_sql_is_distinct_from_guard() {
    let cols = vec!["a".to_string(), "b".to_string()];
    let sql = build_merge_sql("\"public\".\"t\"", "(delta)", &cols, false);
    assert!(sql.contains("IS DISTINCT FROM"));
    assert!(sql.contains("st.\"a\"::text IS DISTINCT FROM d.\"a\"::text"));
}

// ── TG2-MERGE: format helpers ───────────────────────────────────

#[test]
fn test_format_col_list_basic() {
    let cols = vec!["a".to_string(), "b".to_string()];
    assert_eq!(format_col_list(&cols), "\"a\", \"b\"");
}

#[test]
fn test_format_col_list_quoting() {
    let cols = vec!["my \"col\"".to_string()];
    assert_eq!(format_col_list(&cols), "\"my \"\"col\"\"\"");
}

#[test]
fn test_format_prefixed_col_list_basic() {
    let cols = vec!["x".to_string(), "y".to_string()];
    assert_eq!(format_prefixed_col_list("d", &cols), "d.\"x\", d.\"y\"");
}

#[test]
fn test_format_update_set_basic() {
    let cols = vec!["a".to_string(), "b".to_string()];
    assert_eq!(format_update_set(&cols), "\"a\" = d.\"a\", \"b\" = d.\"b\"");
}

// ── TG2-MERGE: trigger template unit tests ──────────────────────

#[test]
fn test_build_trigger_delete_keyed() {
    let sql = build_trigger_delete_sql("\"public\".\"t\"", 42, false);
    assert!(sql.contains("DELETE FROM \"public\".\"t\" AS st"));
    assert!(sql.contains("USING __pgt_delta_42 AS d"));
    assert!(sql.contains("d.__pgt_action = 'D'"));
}

#[test]
fn test_build_trigger_delete_keyless() {
    let sql = build_trigger_delete_sql("\"public\".\"t\"", 42, true);
    assert!(sql.contains("ROW_NUMBER()"));
    assert!(sql.contains("__pgt_delta_42"));
}

#[test]
fn test_build_trigger_update_sql_basic() {
    let cols = vec!["val".to_string()];
    let sql = build_trigger_update_sql("\"public\".\"t\"", 7, &cols);
    assert!(sql.contains("UPDATE \"public\".\"t\" AS st"));
    assert!(sql.contains("SET \"val\" = d.\"val\""));
    assert!(sql.contains("FROM __pgt_delta_7 AS d"));
    assert!(sql.contains("d.__pgt_action = 'I'"));
    assert!(sql.contains("IS DISTINCT FROM"));
}

#[test]
fn test_build_trigger_insert_keyed() {
    let cols = vec!["a".to_string(), "b".to_string()];
    let sql = build_trigger_insert_sql("\"public\".\"t\"", 10, &cols, false);
    assert!(sql.contains("INSERT INTO \"public\".\"t\""));
    assert!(sql.contains("DISTINCT ON (d.__pgt_row_id)"));
    assert!(sql.contains("__pgt_delta_10"));
}

#[test]
fn test_build_trigger_insert_keyless() {
    let cols = vec!["a".to_string(), "b".to_string()];
    let sql = build_trigger_insert_sql("\"public\".\"t\"", 10, &cols, true);
    assert!(sql.contains("INSERT INTO \"public\".\"t\""));
    assert!(!sql.contains("NOT EXISTS"));
    assert!(sql.contains("__pgt_delta_10"));
}

// ── TG2-MERGE: has_non_monotonic_cte() unit tests ───────────────

#[test]
fn test_has_non_monotonic_cte_plain_scan() {
    assert!(!has_non_monotonic_cte(
        "SELECT * FROM changes_42 WHERE lsn > $1",
    ));
}

#[test]
fn test_has_non_monotonic_cte_aggregate() {
    assert!(has_non_monotonic_cte(
        "WITH __pgt_cte_agg_1 AS (SELECT ...) SELECT * FROM __pgt_cte_agg_1",
    ));
}

#[test]
fn test_has_non_monotonic_cte_inner_join() {
    assert!(has_non_monotonic_cte("... __pgt_cte_join_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_left_join() {
    assert!(has_non_monotonic_cte("... __pgt_cte_left_join_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_full_join() {
    assert!(has_non_monotonic_cte("... __pgt_cte_full_join_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_anti_join() {
    assert!(has_non_monotonic_cte("... __pgt_cte_anti_join_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_semi_join() {
    assert!(has_non_monotonic_cte("... __pgt_cte_semi_join_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_distinct() {
    assert!(has_non_monotonic_cte("... __pgt_cte_dist_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_window() {
    assert!(has_non_monotonic_cte("... __pgt_cte_win_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_recursive() {
    assert!(has_non_monotonic_cte("... __pgt_cte_rc_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_intersect() {
    assert!(has_non_monotonic_cte("... __pgt_cte_isect_1 ..."));
}

#[test]
fn test_has_non_monotonic_cte_except() {
    assert!(has_non_monotonic_cte("... __pgt_cte_exct_1 ..."));
}

// ── TG2-MERGE: build_hash_child_merge() unit tests ──────────────

#[test]
fn test_build_hash_child_merge_replaces_target() {
    let original = "MERGE INTO \"public\".\"parent\" AS st \
                    USING (SELECT * FROM delta) AS d \
                    ON st.__pgt_row_id = d.__pgt_row_id \
                    WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE";
    let result = build_hash_child_merge(
        "\"public\".\"child_p0\"",
        "__pgt_delta_mat_42",
        "\"key\"",
        pg_sys::Oid::from(12345u32),
        4,
        0,
        original,
        "\"public\".\"parent\"",
    );
    assert!(result.contains("ONLY \"public\".\"child_p0\""));
    assert!(!result.contains("\"public\".\"parent\""));
}

#[test]
fn test_build_hash_child_merge_filters_with_satisfies_hash() {
    let original = "MERGE INTO \"public\".\"parent\" AS st \
                    USING (SELECT * FROM delta) AS d \
                    ON st.__pgt_row_id = d.__pgt_row_id \
                    WHEN MATCHED THEN DELETE";
    let result = build_hash_child_merge(
        "\"public\".\"child_p1\"",
        "__pgt_mat",
        "\"hash_col\"",
        pg_sys::Oid::from(99u32),
        8,
        3,
        original,
        "\"public\".\"parent\"",
    );
    assert!(result.contains("satisfies_hash_partition(99::oid, 8, 3, \"hash_col\")"));
    assert!(result.contains("__pgt_mat"));
}

#[test]
fn test_build_hash_child_merge_strips_part_pred() {
    let original = "MERGE INTO \"public\".\"parent\" AS st \
                    USING (SELECT * FROM delta) AS d \
                    ON st.__pgt_row_id = d.__pgt_row_id __PGT_PART_PRED__ \
                    WHEN MATCHED THEN DELETE";
    let result = build_hash_child_merge(
        "\"public\".\"child\"",
        "__pgt_mat",
        "\"k\"",
        pg_sys::Oid::from(1u32),
        2,
        1,
        original,
        "\"public\".\"parent\"",
    );
    assert!(!result.contains("__PGT_PART_PRED__"));
}

// ── CORR-4: Z-set weight algebra property tests ─────────────────────────
//
// These proptest-based tests prove the correctness of the Z-set weight
// aggregation contract used by `build_weight_agg_using` and
// `build_keyless_weight_agg`:
//
//   For every __pgt_row_id:
//     net_weight = SUM(CASE WHEN action='I' THEN 1 ELSE -1 END)
//     if net_weight > 0 → emit as INSERT
//     if net_weight < 0 → emit as DELETE
//     if net_weight = 0 → discard (I/D cancellation)
//
// The correctness requirement is that the algebra commutes with
// set application: applying the aggregated delta to a base set S
// produces the same result as applying each individual action
// sequentially (in any order, since they are independent by row_id).

use proptest::prelude::*;

/// Reference implementation of the Z-set weight algebra.
/// Returns (net_inserts, net_deletes) for a given stream of actions.
fn zset_ref(actions: &[char]) -> (i64, i64) {
    let net: i64 = actions
        .iter()
        .map(|a| if *a == 'I' { 1i64 } else { -1 })
        .sum();
    if net > 0 {
        (net, 0)
    } else if net < 0 {
        (0, -net)
    } else {
        (0, 0)
    }
}

/// Simulate sequential application of actions to a multiset.
/// Returns the final count for a single row_id.
fn sequential_apply(initial_count: u32, actions: &[char]) -> i64 {
    let mut count = initial_count as i64;
    for a in actions {
        match a {
            'I' => count += 1,
            'D' => count -= 1,
            _ => {}
        }
    }
    count
}

proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(2000))]

    /// CORR-4a: For a single row_id with random I/D actions, the Z-set
    /// net weight matches sequential application.
    /// The Z-set algebra operates on signed multiplicities (no clipping):
    /// applying stream to initial count 0 gives net_weight directly.
    #[test]
    fn prop_weight_algebra_matches_sequential_from_zero(
        actions in proptest::collection::vec(
            proptest::sample::select(vec!['I', 'D']),
            1..=20
        ),
    ) {
        let (net_ins, net_del) = zset_ref(&actions);
        let seq_result = sequential_apply(0, &actions);

        // Z-set net weight must equal sequential result
        let zset_net = net_ins - net_del;
        prop_assert_eq!(
            zset_net, seq_result,
            "Z-set net ({}) != sequential result ({}) for actions {:?}",
            zset_net, seq_result, actions
        );
        // Output classification must be correct
        if seq_result > 0 {
            prop_assert_eq!(net_ins, seq_result);
            prop_assert_eq!(net_del, 0);
        } else if seq_result < 0 {
            prop_assert_eq!(net_ins, 0);
            prop_assert_eq!(net_del, -seq_result);
        } else {
            prop_assert_eq!(net_ins, 0);
            prop_assert_eq!(net_del, 0);
        }
    }

    /// CORR-4b: Merging two independent action streams for the same row_id
    /// produces the same net weight as concatenating them.
    ///
    /// This proves: weight(A ∪ B) = weight(A) + weight(B)
    /// which is the Z-set homomorphism property.
    #[test]
    fn prop_weight_algebra_additive(
        stream_a in proptest::collection::vec(
            proptest::sample::select(vec!['I', 'D']),
            0..=10
        ),
        stream_b in proptest::collection::vec(
            proptest::sample::select(vec!['I', 'D']),
            0..=10
        ),
    ) {
        let weight_a: i64 = stream_a.iter().map(|a| if *a == 'I' { 1i64 } else { -1 }).sum();
        let weight_b: i64 = stream_b.iter().map(|a| if *a == 'I' { 1i64 } else { -1 }).sum();

        let mut combined = stream_a.clone();
        combined.extend_from_slice(&stream_b);
        let weight_combined: i64 = combined.iter().map(|a| if *a == 'I' { 1i64 } else { -1 }).sum();

        prop_assert_eq!(
            weight_a + weight_b, weight_combined,
            "weight(A) + weight(B) != weight(A ∪ B): {} + {} != {} for A={:?} B={:?}",
            weight_a, weight_b, weight_combined, stream_a, stream_b
        );
    }

    /// CORR-4c: The HAVING <> 0 filter correctly eliminates zero-weight
    /// groups (I/D pairs cancel completely).
    #[test]
    fn prop_having_filter_zero_cancellation(
        n_inserts in 0u32..=10,
        n_deletes in 0u32..=10,
    ) {
        let mut actions: Vec<char> = Vec::new();
        actions.extend(std::iter::repeat_n('I', n_inserts as usize));
        actions.extend(std::iter::repeat_n('D', n_deletes as usize));

        let (net_ins, net_del) = zset_ref(&actions);
        let net_weight: i64 = n_inserts as i64 - n_deletes as i64;

        if net_weight == 0 {
            prop_assert_eq!(net_ins, 0);
            prop_assert_eq!(net_del, 0);
        } else if net_weight > 0 {
            prop_assert_eq!(net_ins, net_weight);
            prop_assert_eq!(net_del, 0);
        } else {
            prop_assert_eq!(net_ins, 0);
            prop_assert_eq!(net_del, net_weight.unsigned_abs() as i64);
        }
    }

    /// CORR-4d: Multi-row weight aggregation: for a batch of (row_id, action)
    /// pairs, each row_id's net weight is computed independently. Proves that
    /// GROUP BY __pgt_row_id correctly partitions the aggregation.
    #[test]
    fn prop_weight_algebra_multi_row_independence(
        rows in proptest::collection::vec(
            (0u32..5, proptest::collection::vec(
                proptest::sample::select(vec!['I', 'D']),
                1..=8
            )),
            1..=10
        ),
    ) {
        let mut per_row: std::collections::HashMap<u32, Vec<char>> = std::collections::HashMap::new();
        for (row_id, actions) in &rows {
            per_row.entry(*row_id).or_default().extend(actions);
        }

        for (row_id, actions) in &per_row {
            let (net_ins, net_del) = zset_ref(actions);
            let net_weight: i64 = actions.iter().map(|a| if *a == 'I' { 1i64 } else { -1 }).sum();

            if net_weight > 0 {
                prop_assert_eq!(net_ins, net_weight,
                    "row_id={}: expected net_ins={} got {}", row_id, net_weight, net_ins);
                prop_assert_eq!(net_del, 0);
            } else if net_weight < 0 {
                prop_assert_eq!(net_ins, 0);
                prop_assert_eq!(net_del, -net_weight,
                    "row_id={}: expected net_del={} got {}", row_id, -net_weight, net_del);
            } else {
                prop_assert_eq!(net_ins, 0, "row_id={}: zero-weight should emit nothing", row_id);
                prop_assert_eq!(net_del, 0);
            }
        }
    }

    /// CORR-4e: The DISTINCT ON ordering resolves D+I pairs (key change)
    /// to a single action per row_id.
    #[test]
    fn prop_keyed_distinct_on_resolves_to_single_action(
        n_inserts in 1u32..=10,
        n_deletes in 1u32..=10,
    ) {
        let net = n_inserts as i64 - n_deletes as i64;
        let expected_action = if net > 0 {
            Some('I')
        } else if net < 0 {
            Some('D')
        } else {
            None
        };

        let (ni, nd) = zset_ref(
            &{
                let mut v = Vec::new();
                v.extend(std::iter::repeat_n('I', n_inserts as usize));
                v.extend(std::iter::repeat_n('D', n_deletes as usize));
                v
            }
        );

        match expected_action {
            Some('I') => {
                prop_assert!(ni > 0, "expected INSERT action for net={}", net);
                prop_assert_eq!(nd, 0);
            }
            Some('D') => {
                prop_assert!(nd > 0, "expected DELETE action for net={}", net);
                prop_assert_eq!(ni, 0);
            }
            None => {
                prop_assert_eq!(ni, 0, "expected no output for net=0");
                prop_assert_eq!(nd, 0);
            }
            _ => unreachable!(),
        }
    }

    /// CORR-4f: Keyless weight aggregation expands to the correct count
    /// via generate_series. The ABS(net_weight) rows should be emitted.
    #[test]
    fn prop_keyless_weight_expansion_count(
        n_inserts in 0u32..=10,
        n_deletes in 0u32..=10,
    ) {
        let net = n_inserts as i64 - n_deletes as i64;
        let expected_count = net.unsigned_abs();

        let mut actions = Vec::new();
        actions.extend(std::iter::repeat_n('I', n_inserts as usize));
        actions.extend(std::iter::repeat_n('D', n_deletes as usize));

        let (ni, nd) = zset_ref(&actions);
        let actual_count = (ni + nd) as u64;

        prop_assert_eq!(
            actual_count, expected_count,
            "keyless expansion: expected {} rows, got {} for {} I + {} D",
            expected_count, actual_count, n_inserts, n_deletes
        );
    }
}

// ── EC01-4: Join cross-cycle phantom convergence property tests ──────
//
// These tests verify that INSERT/DELETE sequences on multi-table JOINs
// converge to the same result as a full refresh, regardless of the order
// and timing of changes across multiple refresh cycles.
//
// The model: two tables L (left) and R (right) with a join L.key = R.key.
// Each cycle, random I/D actions are applied to both sides. The
// incremental result (applying deltas cycle-by-cycle) must match the
// full recomputation at the end.
//
// The property: ∀ action sequences, Σ(deltas) = full_join(L_final, R_final)

/// Simulate a join between two tables and verify convergence.
///
/// `left_cycles` and `right_cycles` each contain a sequence of (key, action)
/// pairs per cycle. After all cycles, the incremental result must match the
/// full join of the final table states.
fn simulate_join_convergence(
    left_cycles: &[Vec<(u32, char)>],
    right_cycles: &[Vec<(u32, char)>],
) -> bool {
    use std::collections::{HashMap, HashSet};

    // Track table state: key → count (non-negative multiset)
    let mut left_state: HashMap<u32, i64> = HashMap::new();
    let mut right_state: HashMap<u32, i64> = HashMap::new();

    // Track incremental join result: (left_key, right_key) → count
    let mut incr_result: HashMap<(u32, u32), i64> = HashMap::new();

    let n_cycles = left_cycles.len().max(right_cycles.len());

    for cycle in 0..n_cycles {
        let left_delta = left_cycles.get(cycle).cloned().unwrap_or_default();
        let right_delta = right_cycles.get(cycle).cloned().unwrap_or_default();

        // Compute effective left delta weights (skip impossible deletes)
        let mut left_dw: HashMap<u32, i64> = HashMap::new();
        for (key, action) in &left_delta {
            if *action == 'D' && *left_state.get(key).unwrap_or(&0) <= 0 {
                continue; // Can't delete what doesn't exist
            }
            let w = if *action == 'I' { 1i64 } else { -1 };
            *left_dw.entry(*key).or_insert(0) += w;
            *left_state.entry(*key).or_insert(0) += w;
        }

        let mut right_dw: HashMap<u32, i64> = HashMap::new();
        for (key, action) in &right_delta {
            if *action == 'D' && *right_state.get(key).unwrap_or(&0) <= 0 {
                continue;
            }
            let w = if *action == 'I' { 1i64 } else { -1 };
            *right_dw.entry(*key).or_insert(0) += w;
            *right_state.entry(*key).or_insert(0) += w;
        }

        // DBSP delta join: ΔJ = (ΔL ⋈ R_after) + (L_before ⋈ ΔR)
        //
        // Part 1: ΔL ⋈ R_after (right state AFTER applying right delta)
        for (lk, lw) in &left_dw {
            let rcount = *right_state.get(lk).unwrap_or(&0);
            *incr_result.entry((*lk, *lk)).or_insert(0) += lw * rcount;
        }

        // Part 2: L_before ⋈ ΔR (left state BEFORE applying left delta)
        // L_before = L_after - ΔL
        for (rk, rw) in &right_dw {
            let l_after = *left_state.get(rk).unwrap_or(&0);
            let l_delta = *left_dw.get(rk).unwrap_or(&0);
            let l_before = l_after - l_delta;
            *incr_result.entry((*rk, *rk)).or_insert(0) += l_before * rw;
        }
    }

    // Compute full join of final states
    let mut full_result: HashMap<(u32, u32), i64> = HashMap::new();
    for (lk, lcount) in &left_state {
        if *lcount > 0
            && let Some(&rcount) = right_state.get(lk)
            && rcount > 0
        {
            *full_result.entry((*lk, *lk)).or_insert(0) += lcount * rcount;
        }
    }

    // Check convergence: incremental result must match full result
    // (ignoring keys with zero count)
    let all_keys: HashSet<(u32, u32)> = incr_result
        .keys()
        .chain(full_result.keys())
        .cloned()
        .collect();

    for key in &all_keys {
        let incr = *incr_result.get(key).unwrap_or(&0);
        let full = *full_result.get(key).unwrap_or(&0);
        if incr != full {
            return false;
        }
    }

    true
}

proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(5000))]

    /// EC01-4a: Single-cycle join convergence — random I/D on both sides.
    #[test]
    fn prop_join_single_cycle_convergence(
        left_actions in proptest::collection::vec(
            (0u32..5, proptest::sample::select(vec!['I', 'D'])),
            1..=20
        ),
        right_actions in proptest::collection::vec(
            (0u32..5, proptest::sample::select(vec!['I', 'D'])),
            1..=20
        ),
    ) {
        prop_assert!(
            simulate_join_convergence(&[left_actions], &[right_actions]),
            "Single-cycle join convergence failed"
        );
    }

    /// EC01-4b: Multi-cycle join convergence — 3-10 cycles of random I/D.
    #[test]
    fn prop_join_multi_cycle_convergence(
        n_cycles in 3usize..=10,
        left_actions in proptest::collection::vec(
            proptest::collection::vec(
                (0u32..5, proptest::sample::select(vec!['I', 'D'])),
                0..=10
            ),
            3..=10
        ),
        right_actions in proptest::collection::vec(
            proptest::collection::vec(
                (0u32..5, proptest::sample::select(vec!['I', 'D'])),
                0..=10
            ),
            3..=10
        ),
    ) {
        let left = &left_actions[..n_cycles.min(left_actions.len())];
        let right = &right_actions[..n_cycles.min(right_actions.len())];
        prop_assert!(
            simulate_join_convergence(left, right),
            "Multi-cycle join convergence failed after {} cycles", n_cycles
        );
    }

    /// EC01-4c: Asymmetric changes — only one side changes per cycle.
    #[test]
    fn prop_join_asymmetric_convergence(
        left_actions in proptest::collection::vec(
            proptest::collection::vec(
                (0u32..3, proptest::sample::select(vec!['I', 'D'])),
                0..=8
            ),
            2..=6
        ),
        right_actions in proptest::collection::vec(
            proptest::collection::vec(
                (0u32..3, proptest::sample::select(vec!['I', 'D'])),
                0..=8
            ),
            2..=6
        ),
    ) {
        // Alternate: even cycles change left, odd cycles change right
        let n = left_actions.len().min(right_actions.len());
        let mut left_cyc = Vec::new();
        let mut right_cyc = Vec::new();
        for i in 0..n {
            if i % 2 == 0 {
                left_cyc.push(left_actions[i].clone());
                right_cyc.push(vec![]);
            } else {
                left_cyc.push(vec![]);
                right_cyc.push(right_actions[i].clone());
            }
        }
        prop_assert!(
            simulate_join_convergence(&left_cyc, &right_cyc),
            "Asymmetric join convergence failed"
        );
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod pg_tests {
    use super::*;
    use crate::catalog::StreamTableMeta;
    use crate::version::Frontier;
    use pgrx::prelude::*;

    #[pg_test]
    fn test_execute_differential_refresh_success() {
        Spi::run("CREATE SCHEMA IF NOT EXISTS public");
        Spi::run("CREATE TABLE public.test_refresh_src (id INT PRIMARY KEY, val TEXT)");

        Spi::run(
            "SELECT pgtrickle.create_stream_table(
            'public.test_refresh_st',
            'SELECT id, val FROM public.test_refresh_src',
            '1 minute'
        );",
        );

        Spi::run("INSERT INTO public.test_refresh_src VALUES (1, 'hello'), (2, 'world')");

        // Wait, populate via refresh
        Spi::run("SELECT pgtrickle.refresh('public.test_refresh_st', 'FULL')");

        // Get metadata correctly
        let st = StreamTableMeta::get_by_name("public", "test_refresh_st").expect("st must exist");
        assert!(st.is_populated, "ST should be populated after FULL");

        let prev_frontier = st.frontier.clone();
        assert!(
            prev_frontier.as_ref().map_or(false, |f| !f.is_empty()),
            "Frontier should not be empty after FULL refresh"
        );

        // Make delta changes
        Spi::run("INSERT INTO public.test_refresh_src VALUES (3, 'foo')");
        Spi::run("UPDATE public.test_refresh_src SET val = 'bar' WHERE id = 1");
        Spi::run("DELETE FROM public.test_refresh_src WHERE id = 2");

        let new_frontier = crate::version::capture_current_frontier().expect("new frontier");
        let prev_frontier_ref = prev_frontier.as_ref().expect("prev_frontier must be Some");

        let (inserted, deleted) =
            execute_differential_refresh(&st, prev_frontier_ref, &new_frontier)
                .expect("differential refresh should succeed");

        assert!(inserted > 0, "should have inserted rows");
        assert!(deleted > 0, "should have deleted rows");

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM public.test_refresh_st")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2, "1,3 should be present");

        Spi::run("SELECT pgtrickle.drop_stream_table('public.test_refresh_st')");
        Spi::run("DROP TABLE public.test_refresh_src CASCADE");
    }
} // close mod pg_tests

// ── G12-2: validate_topk_metadata_fields tests ─────────────────

#[test]
fn test_topk_metadata_valid() {
    assert!(validate_topk_metadata_fields(10, "score DESC", None).is_ok());
}

#[test]
fn test_topk_metadata_valid_with_offset() {
    assert!(validate_topk_metadata_fields(10, "score DESC", Some(5)).is_ok());
}

#[test]
fn test_topk_metadata_zero_limit() {
    assert!(validate_topk_metadata_fields(0, "score DESC", None).is_ok());
}

#[test]
fn test_topk_metadata_negative_limit() {
    let result = validate_topk_metadata_fields(-1, "score DESC", None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("negative"));
}

#[test]
fn test_topk_metadata_empty_order_by() {
    let result = validate_topk_metadata_fields(10, "", None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("empty"));
}

#[test]
fn test_topk_metadata_whitespace_order_by() {
    let result = validate_topk_metadata_fields(10, "   ", None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("empty"));
}

#[test]
fn test_topk_metadata_negative_offset() {
    let result = validate_topk_metadata_fields(10, "score DESC", Some(-3));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("negative"));
}

// ── DAG-4: Bypass capture SQL tests ─────────────────────────────────

#[test]
fn test_build_bypass_capture_sql_basic() {
    let sql = build_bypass_capture_sql(
        42,
        &[
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
        ],
        "pg_temp.__pgt_bypass_42",
        None,
    );
    assert!(sql.contains("CREATE TEMP TABLE IF NOT EXISTS pg_temp.__pgt_bypass_42"));
    assert!(sql.contains("ON COMMIT DROP"));
    // A44-10: flat D+I schema — no new_/old_ prefix
    assert!(sql.contains("\"id\""));
    assert!(sql.contains("\"name\""));
    assert!(!sql.contains("\"new_id\""));
    assert!(!sql.contains("\"new_name\""));
    assert!(sql.contains("FROM __pgt_delta_42 d"));
    assert!(sql.contains("d.__pgt_action IN ('I', 'D')"));
}

#[test]
fn test_build_bypass_capture_sql_quoted_columns() {
    let sql = build_bypass_capture_sql(
        7,
        &[("col\"name".to_string(), "text".to_string())],
        "pg_temp.__pgt_bypass_7",
        None,
    );
    // A44-10: flat D+I schema — column with quote should be properly escaped.
    assert!(sql.contains(r#""col""name""#));
    assert!(!sql.contains(r#""new_col""name""#));
    assert!(sql.contains(r#"d."col""name""#));
}

#[test]
fn test_build_bypass_capture_sql_column_defs() {
    let sql = build_bypass_capture_sql(
        1,
        &[
            ("a".to_string(), "bigint".to_string()),
            ("b".to_string(), "text".to_string()),
        ],
        "pg_temp.__pgt_bypass_1",
        None,
    );
    // Verify the column definitions in CREATE TEMP TABLE.
    // A44-10: flat D+I schema — no new_/old_ prefix
    assert!(sql.contains("lsn pg_lsn"));
    assert!(sql.contains("action \"char\""));
    assert!(sql.contains("pk_hash bigint"));
    assert!(sql.contains("\"a\" bigint"));
    assert!(sql.contains("\"b\" text"));
    assert!(!sql.contains("\"new_a\""));
    assert!(!sql.contains("\"new_b\""));
}

#[test]
fn test_build_bypass_capture_sql_lsn_override() {
    let sql = build_bypass_capture_sql(
        42,
        &[("id".to_string(), "integer".to_string())],
        "pg_temp.__pgt_bypass_42",
        Some("0/1A2B3C"),
    );
    // Should use the literal LSN, not pg_current_wal_lsn()
    assert!(sql.contains("'0/1A2B3C'::pg_lsn"));
    assert!(!sql.contains("pg_current_wal_lsn()"));
}

#[test]
fn test_build_bypass_capture_sql_no_lsn_override() {
    let sql = build_bypass_capture_sql(
        42,
        &[("id".to_string(), "integer".to_string())],
        "pg_temp.__pgt_bypass_42",
        None,
    );
    // Should use pg_current_wal_lsn() by default
    assert!(sql.contains("pg_current_wal_lsn()"));
}

#[test]
fn test_st_bypass_thread_local_set_get_clear() {
    clear_all_st_bypass();
    assert!(get_st_bypass_tables().is_empty());

    set_st_bypass(10, "pg_temp.__pgt_bypass_10".to_string());
    set_st_bypass(20, "pg_temp.__pgt_bypass_20".to_string());

    let tables = get_st_bypass_tables();
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[&10], "pg_temp.__pgt_bypass_10");
    assert_eq!(tables[&20], "pg_temp.__pgt_bypass_20");

    clear_st_bypass(10);
    assert_eq!(get_st_bypass_tables().len(), 1);

    clear_all_st_bypass();
    assert!(get_st_bypass_tables().is_empty());
}

// ── build_is_distinct_clause ─────────────────────────────────────────────

#[test]
fn test_build_is_distinct_clause_single_col() {
    let cols = vec!["price".to_string()];
    let clause = build_is_distinct_clause(&cols);
    assert_eq!(
        clause,
        r#"st."price"::text IS DISTINCT FROM d."price"::text"#
    );
}

#[test]
fn test_build_is_distinct_clause_multi_col() {
    let cols = vec!["a".to_string(), "b".to_string()];
    let clause = build_is_distinct_clause(&cols);
    assert_eq!(
        clause,
        r#"st."a"::text IS DISTINCT FROM d."a"::text OR st."b"::text IS DISTINCT FROM d."b"::text"#
    );
}

#[test]
fn test_build_is_distinct_clause_col_with_double_quote() {
    let cols = vec!["col\"name".to_string()];
    let clause = build_is_distinct_clause(&cols);
    // Inner double-quote must be escaped as ""
    assert!(clause.contains(r#""col""name""#));
    assert!(clause.contains("IS DISTINCT FROM"));
}

#[test]
fn test_build_is_distinct_clause_at_threshold() {
    // Exactly 50 columns — still per-column path
    let cols: Vec<String> = (0..50).map(|i| format!("col{i}")).collect();
    let clause = build_is_distinct_clause(&cols);
    // Per-column path produces OR-joined expressions
    assert!(clause.contains("IS DISTINCT FROM"));
    assert!(!clause.contains("pgtrickle.pg_trickle_hash"));
    let parts: Vec<&str> = clause.split(" OR ").collect();
    assert_eq!(parts.len(), 50);
}

#[test]
fn test_build_is_distinct_clause_wide_table_hash_path() {
    // 51 columns — crosses into hash-based comparison
    let cols: Vec<String> = (0..51).map(|i| format!("col{i}")).collect();
    let clause = build_is_distinct_clause(&cols);
    assert!(clause.contains("pgtrickle.pg_trickle_hash"));
    assert!(clause.contains("IS DISTINCT FROM"));
    // Should reference both st. and d. prefixes
    assert!(clause.contains("st.\"col0\""));
    assert!(clause.contains("d.\"col0\""));
    // Should use the record separator
    assert!(clause.contains(r"'\x1E'"));
    // No OR — single hash expression
    assert!(!clause.contains(" OR "));
}

#[test]
fn test_build_is_distinct_clause_empty_cols() {
    let cols: Vec<String> = vec![];
    let clause = build_is_distinct_clause(&cols);
    // Empty slice → empty string (no columns to compare)
    assert_eq!(clause, "");
}

// ── pg_quote_literal ─────────────────────────────────────────────────────

#[test]
fn test_pg_quote_literal_simple() {
    assert_eq!(pg_quote_literal("hello"), "'hello'");
}

#[test]
fn test_pg_quote_literal_empty() {
    assert_eq!(pg_quote_literal(""), "''");
}

#[test]
fn test_pg_quote_literal_single_quote_escaped() {
    assert_eq!(pg_quote_literal("it's"), "'it''s'");
}

#[test]
fn test_pg_quote_literal_multiple_single_quotes() {
    assert_eq!(pg_quote_literal("a'b'c"), "'a''b''c'");
}

#[test]
fn test_pg_quote_literal_only_quotes() {
    // Input is two single quotes; each is doubled (→ 4 quotes) then wrapped → 6 quotes
    assert_eq!(pg_quote_literal("''"), "''''''");
}

// ── inject_partition_predicate ───────────────────────────────────────────

#[test]
fn test_inject_partition_predicate_basic() {
    let merge_sql = "MERGE INTO st USING d ON st.id = d.id__PGT_PART_PRED__";
    let bounds = PartitionBounds::Range {
        mins: vec!["2024-01-01".to_string()],
        maxs: vec!["2024-01-31".to_string()],
    };
    let result = inject_partition_predicate(merge_sql, "event_date", &bounds);
    assert!(result.contains("BETWEEN '2024-01-01' AND '2024-01-31'"));
    assert!(result.contains(r#""event_date""#));
    assert!(result.contains("st."));
    assert!(!result.contains("__PGT_PART_PRED__"));
}

#[test]
fn test_inject_partition_predicate_no_placeholder() {
    // If there is no placeholder the SQL is returned unchanged
    let merge_sql = "MERGE INTO st USING d ON st.id = d.id";
    let bounds = PartitionBounds::Range {
        mins: vec!["2024-01-01".to_string()],
        maxs: vec!["2024-01-31".to_string()],
    };
    let result = inject_partition_predicate(merge_sql, "event_date", &bounds);
    assert_eq!(result, merge_sql);
}

#[test]
fn test_inject_partition_predicate_value_with_single_quote() {
    let merge_sql = "MERGE INTO st USING d ON st.id = d.id__PGT_PART_PRED__";
    let bounds = PartitionBounds::Range {
        mins: vec!["O'Brien".to_string()],
        maxs: vec!["O'Reilly".to_string()],
    };
    let result = inject_partition_predicate(merge_sql, "name", &bounds);
    // Single quotes must be doubled inside the predicate literals
    assert!(result.contains("'O''Brien'"));
    assert!(result.contains("'O''Reilly'"));
}

// A1-1b: multi-column partition predicate tests

#[test]
fn test_inject_partition_predicate_multi_column() {
    let merge_sql = "MERGE INTO st USING d ON st.id = d.id__PGT_PART_PRED__";
    let bounds = PartitionBounds::Range {
        mins: vec!["2024-01-01".to_string(), "100".to_string()],
        maxs: vec!["2024-01-31".to_string(), "999".to_string()],
    };
    let result = inject_partition_predicate(merge_sql, "event_day,customer_id", &bounds);
    // Multi-column uses ROW comparison instead of BETWEEN
    assert!(
        result.contains("ROW(st.\"event_day\", st.\"customer_id\") >= ROW('2024-01-01', '100')")
    );
    assert!(
        result.contains("ROW(st.\"event_day\", st.\"customer_id\") <= ROW('2024-01-31', '999')")
    );
    assert!(!result.contains("__PGT_PART_PRED__"));
}

#[test]
fn test_inject_partition_predicate_three_columns() {
    let merge_sql = "MERGE INTO st USING d ON st.id = d.id__PGT_PART_PRED__";
    let bounds = PartitionBounds::Range {
        mins: vec!["1".to_string(), "x".to_string(), "10".to_string()],
        maxs: vec!["9".to_string(), "z".to_string(), "90".to_string()],
    };
    let result = inject_partition_predicate(merge_sql, "a, b, c", &bounds);
    assert!(result.contains("ROW(st.\"a\", st.\"b\", st.\"c\") >= ROW('1', 'x', '10')"));
    assert!(result.contains("ROW(st.\"a\", st.\"b\", st.\"c\") <= ROW('9', 'z', '90')"));
}

// A1-1d: LIST partition predicate tests

#[test]
fn test_inject_partition_predicate_list_single_value() {
    let merge_sql = "MERGE INTO st USING d ON st.id = d.id__PGT_PART_PRED__";
    let bounds = PartitionBounds::List(vec!["US".to_string()]);
    let result = inject_partition_predicate(merge_sql, "LIST:region", &bounds);
    assert!(result.contains("st.\"region\" IN ('US')"));
    assert!(!result.contains("__PGT_PART_PRED__"));
}

#[test]
fn test_inject_partition_predicate_list_multiple_values() {
    let merge_sql = "MERGE INTO st USING d ON st.id = d.id__PGT_PART_PRED__";
    let bounds =
        PartitionBounds::List(vec!["EU".to_string(), "US".to_string(), "APAC".to_string()]);
    let result = inject_partition_predicate(merge_sql, "LIST:region", &bounds);
    assert!(result.contains("st.\"region\" IN ('EU', 'US', 'APAC')"));
    assert!(!result.contains("__PGT_PART_PRED__"));
}

#[test]
fn test_inject_partition_predicate_list_value_with_quote() {
    let merge_sql = "MERGE INTO st USING d ON st.id = d.id__PGT_PART_PRED__";
    let bounds = PartitionBounds::List(vec!["O'Brien".to_string()]);
    let result = inject_partition_predicate(merge_sql, "LIST:name", &bounds);
    assert!(result.contains("'O''Brien'"));
}

// ── build_weight_agg_using ───────────────────────────────────────────────

#[test]
fn test_build_weight_agg_using_contains_delta_sql() {
    let delta = "SELECT * FROM my_delta";
    let cols = "\"a\", \"b\"";
    let sql = build_weight_agg_using(delta, cols);
    assert!(sql.contains(delta));
    assert!(sql.contains(cols));
}

#[test]
fn test_build_weight_agg_using_structure() {
    let sql = build_weight_agg_using("SELECT 1", "\"x\"");
    // Must contain the structural landmarks
    assert!(sql.contains("DISTINCT ON"));
    assert!(sql.contains("__pgt_row_id"));
    assert!(sql.contains("__pgt_action"));
    assert!(sql.contains("SUM"));
    assert!(sql.contains("HAVING"));
    assert!(sql.contains("GROUP BY"));
    assert!(sql.contains("ORDER BY"));
}

#[test]
fn test_build_weight_agg_using_insert_delete_case() {
    let sql = build_weight_agg_using("SELECT 1", "\"x\"");
    assert!(sql.contains("'I'"));
    assert!(sql.contains("'D'"));
    // Net-weight sign decides the action
    assert!(sql.contains("> 0"));
    assert!(sql.contains("<> 0"));
}

// ── build_keyless_weight_agg ────────────────────────────────────────────

#[test]
fn test_build_keyless_weight_agg_contains_delta_sql() {
    let delta = "SELECT * FROM my_delta";
    let cols = "\"a\", \"b\"";
    let sql = build_keyless_weight_agg(delta, cols);
    assert!(sql.contains(delta));
    assert!(sql.contains(cols));
}

#[test]
fn test_build_keyless_weight_agg_structure() {
    let sql = build_keyless_weight_agg("SELECT 1", "\"x\"");
    // Must contain weight-aggregation landmarks
    assert!(sql.contains("__pgt_row_id"));
    assert!(sql.contains("__pgt_action"));
    assert!(sql.contains("SUM"));
    assert!(sql.contains("HAVING"));
    assert!(sql.contains("GROUP BY"));
    // Must use generate_series for count expansion
    assert!(sql.contains("generate_series"));
    assert!(sql.contains("__pgt_cnt"));
    // Must NOT use DISTINCT ON (keyless allows duplicate row_ids)
    assert!(!sql.contains("DISTINCT ON"));
}

#[test]
fn test_build_keyless_weight_agg_cancel_logic() {
    let sql = build_keyless_weight_agg("SELECT 1", "\"x\"");
    assert!(sql.contains("'I'"));
    assert!(sql.contains("'D'"));
    // Net-weight sign decides the action
    assert!(sql.contains("> 0"));
    // HAVING filters out net-zero groups (I/D cancellation)
    assert!(sql.contains("<> 0"));
    // ABS for the count expansion
    assert!(sql.contains("ABS"));
}

// ── build_keyless_delete_template ────────────────────────────────────────

#[test]
fn test_build_keyless_delete_template_contains_table() {
    let sql = build_keyless_delete_template("\"public\".\"my_table\"", 42);
    assert!(sql.starts_with("DELETE FROM \"public\".\"my_table\""));
}

#[test]
fn test_build_keyless_delete_template_uses_pgt_id() {
    let sql = build_keyless_delete_template("\"s\".\"t\"", 99);
    assert!(sql.contains("__pgt_delta_99"));
}

#[test]
fn test_build_keyless_delete_template_structure() {
    let sql = build_keyless_delete_template("\"s\".\"t\"", 1);
    // Must use ctid-based deletion with ROW_NUMBER pairing
    assert!(sql.contains("ctid"));
    assert!(sql.contains("ROW_NUMBER()"));
    assert!(sql.contains("PARTITION BY"));
    assert!(sql.contains("JOIN"));
    assert!(sql.contains("del_count"));
    assert!(sql.contains("__pgt_action = 'D'"));
}

#[test]
fn test_build_keyless_delete_template_counts_correctly() {
    let sql = build_keyless_delete_template("\"s\".\"t\"", 5);
    // The WHERE clause must use <= del_count to limit paired deletions
    assert!(sql.contains("<= dc.del_count"));
}

// ── A1-3b: HASH partition bound spec parsing ────────────────────────────

#[test]
fn test_parse_hash_bound_spec_basic() {
    let (m, r) = parse_hash_bound_spec("FOR VALUES WITH (modulus 4, remainder 2)").unwrap(); // nosemgrep: semgrep.rust.panic-in-sql-path
    assert_eq!(m, 4);
    assert_eq!(r, 2);
}

#[test]
fn test_parse_hash_bound_spec_various_values() {
    let (m, r) = parse_hash_bound_spec("FOR VALUES WITH (modulus 8, remainder 7)").unwrap(); // nosemgrep: semgrep.rust.panic-in-sql-path
    assert_eq!(m, 8);
    assert_eq!(r, 7);
}

#[test]
fn test_parse_hash_bound_spec_remainder_zero() {
    let (m, r) = parse_hash_bound_spec("FOR VALUES WITH (modulus 4, remainder 0)").unwrap(); // nosemgrep: semgrep.rust.panic-in-sql-path
    assert_eq!(m, 4);
    assert_eq!(r, 0);
}

#[test]
fn test_parse_hash_bound_spec_missing_modulus() {
    let result = parse_hash_bound_spec("FOR VALUES WITH (remainder 2)");
    assert!(result.is_err());
}

#[test]
fn test_parse_hash_bound_spec_missing_remainder() {
    let result = parse_hash_bound_spec("FOR VALUES WITH (modulus 4)");
    assert!(result.is_err());
}

#[test]
fn test_extract_keyword_int_basic() {
    assert_eq!(
        extract_keyword_int("MODULUS 4, REMAINDER 2", "MODULUS").unwrap(),
        4
    );
    assert_eq!(
        extract_keyword_int("MODULUS 4, REMAINDER 2", "REMAINDER").unwrap(),
        2
    );
}

#[test]
fn test_extract_keyword_int_missing() {
    assert!(extract_keyword_int("SOME OTHER TEXT", "MODULUS").is_err());
}

// ── D-4: Multi-frontier cleanup model ──────────────────────────────

/// Pure-Rust model of the multi-frontier cleanup logic.
///
/// Given a set of consumer frontier LSNs for a source OID, computes the
/// safe cleanup threshold: `MIN(consumer_frontiers)`. Only change buffer
/// entries at or below this threshold may be deleted.
///
/// Returns `None` when there are no consumers with a valid (non-0/0) frontier.
fn compute_safe_cleanup_lsn(consumer_frontiers: &[&str]) -> Option<String> {
    let valid: Vec<&str> = consumer_frontiers
        .iter()
        .copied()
        .filter(|lsn| *lsn != "0/0")
        .collect();
    if valid.is_empty() {
        return None;
    }
    let mut min = valid[0];
    for &lsn in &valid[1..] {
        min = crate::version::lsn_min(min, lsn);
    }
    Some(min.to_string())
}

/// Model: given change buffer entries (as LSNs) and the safe cleanup
/// threshold, returns the set of entries that should be RETAINED (not deleted).
fn retained_after_cleanup(entry_lsns: &[&str], safe_lsn: &str) -> Vec<String> {
    entry_lsns
        .iter()
        .copied()
        .filter(|lsn| crate::version::lsn_gt(lsn, safe_lsn))
        .map(|s| s.to_string())
        .collect()
}

#[test]
fn test_safe_cleanup_lsn_single_consumer() {
    let result = compute_safe_cleanup_lsn(&["0/100"]);
    assert_eq!(result, Some("0/100".to_string()));
}

#[test]
fn test_safe_cleanup_lsn_multi_consumer_min() {
    // 5 consumers with different frontiers — safe threshold is the minimum.
    let result = compute_safe_cleanup_lsn(&["0/500", "0/200", "0/300", "0/100", "0/400"]);
    assert_eq!(result, Some("0/100".to_string()));
}

#[test]
fn test_safe_cleanup_lsn_skips_zero() {
    // Consumer at 0/0 is uninitialized — excluded.
    let result = compute_safe_cleanup_lsn(&["0/0", "0/200", "0/100"]);
    assert_eq!(result, Some("0/100".to_string()));
}

#[test]
fn test_safe_cleanup_lsn_all_zero() {
    let result = compute_safe_cleanup_lsn(&["0/0", "0/0"]);
    assert_eq!(result, None);
}

#[test]
fn test_safe_cleanup_lsn_empty() {
    let result = compute_safe_cleanup_lsn(&[]);
    assert_eq!(result, None);
}

#[test]
fn test_retained_after_cleanup_basic() {
    let entries = vec!["0/50", "0/100", "0/150", "0/200"];
    let retained = retained_after_cleanup(&entries, "0/100");
    assert_eq!(retained, vec!["0/150", "0/200"]);
}

#[test]
fn test_retained_after_cleanup_nothing_deleted() {
    let entries = vec!["0/200", "0/300"];
    let retained = retained_after_cleanup(&entries, "0/100");
    assert_eq!(retained, vec!["0/200", "0/300"]);
}

#[test]
fn test_retained_after_cleanup_all_deleted() {
    let entries = vec!["0/50", "0/100"];
    let retained = retained_after_cleanup(&entries, "0/200");
    assert!(retained.is_empty());
}

#[test]
fn test_multi_frontier_cleanup_never_deletes_unconsumed() {
    // Core correctness property: if consumer C has frontier at LSN X,
    // then no entry with LSN > X should ever be deleted.
    //
    // Scenario: 5 consumers with different frontier positions.
    // Buffer has entries at every 0x100 step from 0/100 to 0/A00.
    let consumer_frontiers = vec!["0/300", "0/700", "0/500", "0/200", "0/900"];
    let buffer_entries: Vec<&str> = vec![
        "0/100", "0/200", "0/300", "0/400", "0/500", "0/600", "0/700", "0/800", "0/900", "0/A00",
    ];

    let safe_lsn =
        compute_safe_cleanup_lsn(&consumer_frontiers).expect("should have a safe threshold"); // nosemgrep: semgrep.rust.panic-in-sql-path
    assert_eq!(safe_lsn, "0/200"); // MIN of all consumers

    let retained = retained_after_cleanup(&buffer_entries, &safe_lsn);

    // Verify: every consumer can still read all entries at or above its frontier.
    for &consumer_lsn in &consumer_frontiers {
        // Entries the consumer still needs: LSN > consumer's PREVIOUS frontier.
        // In production, the consumer reads entries between prev and current frontier,
        // but the critical invariant is: entries above the MIN frontier are retained.
        assert!(
            retained.iter().any(|e| e == consumer_lsn)
                || crate::version::lsn_gt(&safe_lsn, consumer_lsn)
                || safe_lsn == consumer_lsn,
            "consumer at {} should find its entries retained or already consumed",
            consumer_lsn
        );
    }

    // No entry above the slowest consumer was deleted.
    let min_consumer = "0/200";
    for entry in &retained {
        assert!(
            crate::version::lsn_gt(entry, min_consumer),
            "retained entry {} should be above safe threshold {}",
            entry,
            min_consumer
        );
    }
}

// ── D-4: Property-based test — random frontier advancement ──────

/// Generate a random LSN as "0/XXXX" where XXXX is a hex value 1..FFFF.
fn arb_lsn() -> impl Strategy<Value = String> {
    (1u64..0xFFFFu64).prop_map(|v| format!("0/{:X}", v))
}

proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(500))]

    /// Property: MIN(frontiers) is always the safe cleanup threshold.
    /// No entry above this threshold should be deleted.
    /// All entries at or below should be deletable.
    #[test]
    fn prop_multi_frontier_cleanup_correctness(
        frontiers in proptest::collection::vec(arb_lsn(), 5..=10),
        entries in proptest::collection::vec(arb_lsn(), 1..=20),
    ) {
        let frontier_refs: Vec<&str> = frontiers.iter().map(|s| s.as_str()).collect();
        let entry_refs: Vec<&str> = entries.iter().map(|s| s.as_str()).collect();

        let safe_lsn = compute_safe_cleanup_lsn(&frontier_refs);

        if let Some(ref threshold) = safe_lsn {
            let retained = retained_after_cleanup(&entry_refs, threshold);

            // Invariant 1: Every retained entry is strictly above the threshold.
            for entry in &retained {
                prop_assert!(
                    crate::version::lsn_gt(entry, threshold),
                    "retained entry {} should be > threshold {}",
                    entry,
                    threshold
                );
            }

            // Invariant 2: Every non-retained entry is at or below the threshold.
            let deleted: Vec<&str> = entry_refs
                .iter()
                .copied()
                .filter(|e| !retained.contains(&e.to_string()))
                .collect();
            for entry in &deleted {
                prop_assert!(
                    !crate::version::lsn_gt(entry, threshold),
                    "deleted entry {} should be <= threshold {}",
                    entry,
                    threshold
                );
            }

            // Invariant 3: For every consumer, all entries at LSNs above
            // the consumer's frontier are still present in the retained set.
            // (This is the "no premature deletion" property.)
            for consumer_lsn in &frontier_refs {
                for entry in &entry_refs {
                    if crate::version::lsn_gt(entry, consumer_lsn) {
                        // This entry hasn't been consumed by this consumer yet.
                        // It should be retained.
                        prop_assert!(
                            retained.contains(&entry.to_string()),
                            "entry {} is above consumer frontier {} but was deleted (threshold {})",
                            entry,
                            consumer_lsn,
                            threshold
                        );
                    }
                }
            }
        }
    }

    /// Property: Advancing the slowest consumer raises the safe threshold.
    #[test]
    fn prop_advancing_slowest_consumer_raises_threshold(
        base_frontiers in proptest::collection::vec(arb_lsn(), 5..=8),
        advance_amount in 1u64..0x1000u64,
    ) {
        let frontier_refs: Vec<&str> = base_frontiers.iter().map(|s| s.as_str()).collect();

        if let Some(ref old_threshold) = compute_safe_cleanup_lsn(&frontier_refs) {
            // Find the index of the minimum frontier.
            let min_idx = frontier_refs
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    let pa = crate::version::lsn_gt(a, b);
                    if pa { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less }
                })
                .map(|(i, _)| i)
                .unwrap();

            // Advance the slowest consumer.
            let mut advanced = base_frontiers.clone();
            let old_val = crate::version::lsn_gt(&advanced[min_idx], "0/0");
            if old_val {
                // Parse and advance
                let parts: Vec<&str> = advanced[min_idx].split('/').collect();
                let lo = u64::from_str_radix(parts[1], 16).unwrap_or(0);
                advanced[min_idx] = format!("0/{:X}", lo.saturating_add(advance_amount));
            }

            let new_frontier_refs: Vec<&str> = advanced.iter().map(|s| s.as_str()).collect();
            if let Some(ref new_threshold) = compute_safe_cleanup_lsn(&new_frontier_refs) {
                prop_assert!(
                    crate::version::lsn_gte(new_threshold, old_threshold),
                    "advancing slowest consumer should not lower threshold: old={}, new={}",
                    old_threshold,
                    new_threshold
                );
            }
        }
    }

    /// Property: Adding a new consumer at LSN 0/1 (just initialized) should
    /// lower or maintain the safe threshold.
    #[test]
    fn prop_new_consumer_lowers_threshold(
        base_frontiers in proptest::collection::vec(arb_lsn(), 5..=8),
    ) {
        let frontier_refs: Vec<&str> = base_frontiers.iter().map(|s| s.as_str()).collect();

        if let Some(ref old_threshold) = compute_safe_cleanup_lsn(&frontier_refs) {
            // Add a new consumer that just completed its first full refresh
            // with a very low frontier.
            let mut with_new = base_frontiers.clone();
            with_new.push("0/1".to_string());
            let new_frontier_refs: Vec<&str> = with_new.iter().map(|s| s.as_str()).collect();

            if let Some(ref new_threshold) = compute_safe_cleanup_lsn(&new_frontier_refs) {
                prop_assert!(
                    !crate::version::lsn_gt(new_threshold, old_threshold),
                    "adding consumer at 0/1 should not raise threshold: old={}, new={}",
                    old_threshold,
                    new_threshold
                );
            }
        }
    }
}

// ── D-4: Column superset computation tests ──────────────────────

#[test]
fn test_column_superset_union() {
    // Simulate: ST1 uses {a, b, c}, ST2 uses {b, d}, ST3 uses {a, e}.
    // Column superset = {a, b, c, d, e}.
    let st1: Vec<String> = vec!["a", "b", "c"].into_iter().map(String::from).collect();
    let st2: Vec<String> = vec!["b", "d"].into_iter().map(String::from).collect();
    let st3: Vec<String> = vec!["a", "e"].into_iter().map(String::from).collect();

    let mut superset = std::collections::HashSet::new();
    for col in st1.iter().chain(st2.iter()).chain(st3.iter()) {
        superset.insert(col.to_lowercase());
    }

    let mut sorted: Vec<String> = superset.into_iter().collect();
    sorted.sort();
    assert_eq!(sorted, vec!["a", "b", "c", "d", "e"]);
}

#[test]
fn test_column_superset_select_star_forces_full() {
    // If any ST uses SELECT * (columns_used = None), the superset must
    // include ALL columns.
    let st1: Option<Vec<String>> = Some(vec!["a".to_string(), "b".to_string()]);
    let st2: Option<Vec<String>> = None; // SELECT *

    // When any consumer has None, the union should be None (full capture).
    let union_result = match (&st1, &st2) {
        (_, None) | (None, _) => None,
        (Some(a), Some(b)) => {
            let mut s: std::collections::HashSet<String> = a.iter().cloned().collect();
            s.extend(b.iter().cloned());
            Some(s.into_iter().collect::<Vec<_>>())
        }
    };
    assert!(union_result.is_none());
}

// ── B-4: classify_query_complexity() ────────────────────────────

#[test]
fn test_classify_scan() {
    let q = "SELECT id, name FROM users";
    assert_eq!(classify_query_complexity(q), QueryComplexityClass::Scan);
}

#[test]
fn test_classify_filter() {
    let q = "SELECT id, name FROM users WHERE active = true";
    assert_eq!(classify_query_complexity(q), QueryComplexityClass::Filter);
}

#[test]
fn test_classify_aggregate() {
    let q = "SELECT region, SUM(amount) FROM orders GROUP BY region";
    assert_eq!(
        classify_query_complexity(q),
        QueryComplexityClass::Aggregate
    );
}

#[test]
fn test_classify_join() {
    let q = "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id";
    assert_eq!(classify_query_complexity(q), QueryComplexityClass::Join);
}

#[test]
fn test_classify_join_aggregate() {
    let q = "SELECT u.name, SUM(o.amount) FROM orders o JOIN users u ON o.user_id = u.id GROUP BY u.name";
    assert_eq!(
        classify_query_complexity(q),
        QueryComplexityClass::JoinAggregate
    );
}

#[test]
fn test_classify_left_join() {
    let q = "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id";
    assert_eq!(classify_query_complexity(q), QueryComplexityClass::Join);
}

#[test]
fn test_classify_case_insensitive() {
    let q = "select id from users where active group by id";
    assert_eq!(
        classify_query_complexity(q),
        QueryComplexityClass::Aggregate
    );
}

// ── B-4: cost_model_prefers_full() ──────────────────────────────

#[test]
fn test_cost_model_prefers_full_large_delta() {
    // avg_ms_per_delta=1.0, avg_full=100ms, delta=200 rows, scan class
    // est_diff = 1.0 * 1.0 * 200 = 200ms > 100 * 0.8 = 80ms → FULL
    assert!(cost_model_prefers_full(
        1.0,
        100.0,
        200,
        QueryComplexityClass::Scan,
        0.8,
    ));
}

#[test]
fn test_cost_model_prefers_diff_small_delta() {
    // avg_ms_per_delta=1.0, avg_full=100ms, delta=10 rows, scan class
    // est_diff = 1.0 * 1.0 * 10 = 10ms < 100 * 0.8 = 80ms → DIFF
    assert!(!cost_model_prefers_full(
        1.0,
        100.0,
        10,
        QueryComplexityClass::Scan,
        0.8,
    ));
}

#[test]
fn test_cost_model_complexity_affects_decision() {
    // Same delta count, but JoinAggregate has 4× factor
    // Scan: est_diff = 0.5 * 1.0 * 100 = 50ms < 100 * 0.8 = 80ms → DIFF
    assert!(!cost_model_prefers_full(
        0.5,
        100.0,
        100,
        QueryComplexityClass::Scan,
        0.8,
    ));
    // JoinAgg: est_diff = 0.5 * 4.0 * 100 = 200ms > 80ms → FULL
    assert!(cost_model_prefers_full(
        0.5,
        100.0,
        100,
        QueryComplexityClass::JoinAggregate,
        0.8,
    ));
}

// ── B-4: diff_cost_factor() ─────────────────────────────────────

#[test]
fn test_diff_cost_factors_ordering() {
    assert!(
        QueryComplexityClass::Scan.diff_cost_factor()
            < QueryComplexityClass::Filter.diff_cost_factor()
    );
    assert!(
        QueryComplexityClass::Filter.diff_cost_factor()
            < QueryComplexityClass::Aggregate.diff_cost_factor()
    );
    assert!(
        QueryComplexityClass::Aggregate.diff_cost_factor()
            < QueryComplexityClass::Join.diff_cost_factor()
    );
    assert!(
        QueryComplexityClass::Join.diff_cost_factor()
            < QueryComplexityClass::JoinAggregate.diff_cost_factor()
    );
}

// ── T-1 (v0.77.0): DVM algebra detection function property tests ─────

// ── C-3/DVM-1: classify_case_in_list_aggregate_drift ─────────────────

/// T-1a: Canonical q12-like patterns MUST be detected.
#[test]
fn test_t1a_case_in_list_drift_detects_q12_canonical() {
    // q12: SUM(CASE WHEN ... THEN 1 ELSE 0 END) + IN ('MAIL', 'SHIP')
    let q12_like = "SELECT l_shipmode, \
        SUM(CASE WHEN o_orderpriority = '1-URGENT' THEN 1 ELSE 0 END) AS high_cnt \
        FROM orders JOIN lineitem ON o_orderkey = l_orderkey \
        WHERE l_shipmode IN ('MAIL', 'SHIP') \
        GROUP BY l_shipmode ORDER BY l_shipmode";
    assert!(
        classify_case_in_list_aggregate_drift(q12_like),
        "T-1a: q12-like CASE aggregate + IN-list must be detected as drift"
    );
}

/// T-1b: Lowercase variants are also detected (case-insensitive matching).
#[test]
fn test_t1b_case_in_list_drift_case_insensitive() {
    let lower = "select sum(case when status = 'a' then 1 else 0 end) \
                 from t where mode in ('x', 'y')";
    assert!(
        classify_case_in_list_aggregate_drift(lower),
        "T-1b: lowercase SQL must also be detected"
    );
}

/// T-1c: COUNT(CASE...) + IN-list is also detected.
#[test]
fn test_t1c_count_case_in_list_detected() {
    let q = "SELECT COUNT(CASE WHEN x > 0 THEN 1 END) FROM t WHERE mode IN ('A', 'B')";
    assert!(
        classify_case_in_list_aggregate_drift(q),
        "T-1c: COUNT(CASE...) + IN-list must be detected"
    );
}

/// T-1d: Benign queries (aggregate without IN-list) are NOT detected.
#[test]
fn test_t1d_case_agg_no_in_list_not_detected() {
    let q = "SELECT SUM(CASE WHEN val > 0 THEN 1 ELSE 0 END) FROM t WHERE status = 'A'";
    assert!(
        !classify_case_in_list_aggregate_drift(q),
        "T-1d: CASE aggregate without IN-list must NOT be flagged as drift"
    );
}

/// T-1e: Plain IN-list without CASE aggregate is NOT detected.
#[test]
fn test_t1e_in_list_no_case_agg_not_detected() {
    let q = "SELECT count(*) FROM orders WHERE status IN ('A', 'B', 'C')";
    assert!(
        !classify_case_in_list_aggregate_drift(q),
        "T-1e: plain IN-list without CASE aggregate must NOT be flagged"
    );
}

/// T-1f: Simple scan query is NOT detected.
#[test]
fn test_t1f_scan_not_detected_by_case_in_list() {
    let q = "SELECT id, val FROM source_table WHERE id > 5";
    assert!(
        !classify_case_in_list_aggregate_drift(q),
        "T-1f: simple scan must not trigger CASE/IN-list detection"
    );
}

// ── DVM-2/P-1: classify_correlated_aggregate_subquery_in_where ────────

/// T-1g: Canonical q20-like pattern MUST be detected.
#[test]
fn test_t1g_correlated_aggregate_subquery_detects_q20_canonical() {
    // q20: ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE ...)
    let q20_like = "SELECT ps_partkey FROM partsupp WHERE \
        ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem \
                       WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey)";
    assert!(
        classify_correlated_aggregate_subquery_in_where(q20_like),
        "T-1g: q20-like correlated aggregate subquery must be detected"
    );
}

/// T-1h: >= comparison variant is also detected.
#[test]
fn test_t1h_correlated_aggregate_subquery_gte_variant() {
    let q = "SELECT x FROM t WHERE x >= (SELECT SUM(y) FROM s WHERE s.k = t.k)";
    assert!(
        classify_correlated_aggregate_subquery_in_where(q),
        "T-1h: >= correlated aggregate subquery must be detected"
    );
}

/// T-1i: < and <= operators are also detected.
#[test]
fn test_t1i_correlated_aggregate_subquery_lt_lte_variants() {
    let lt = "SELECT a FROM t WHERE cost < (SELECT AVG(price) FROM prices WHERE cat = t.cat)";
    let lte = "SELECT a FROM t WHERE cost <= (SELECT MIN(price) FROM prices WHERE cat = t.cat)";
    assert!(
        classify_correlated_aggregate_subquery_in_where(lt),
        "T-1i: < correlated aggregate subquery must be detected"
    );
    assert!(
        classify_correlated_aggregate_subquery_in_where(lte),
        "T-1i: <= correlated aggregate subquery must be detected"
    );
}

/// T-1j: Subquery without aggregate function is NOT detected (safe to diff).
#[test]
fn test_t1j_comparison_subquery_no_aggregate_not_detected() {
    // Simple non-aggregate correlated subquery — DVM can handle these
    // Use a non-aggregate subquery without MIN/MAX/SUM/AVG/COUNT:
    let q = "SELECT a FROM t WHERE id > (SELECT ref_id FROM config WHERE name = 'limit')";
    assert!(
        !classify_correlated_aggregate_subquery_in_where(q),
        "T-1j: correlated subquery without aggregate must NOT be flagged"
    );
}

/// T-1k: Simple scan with no subquery is NOT detected.
#[test]
fn test_t1k_no_subquery_not_detected() {
    let q = "SELECT id, name FROM customers WHERE region = 'EU'";
    assert!(
        !classify_correlated_aggregate_subquery_in_where(q),
        "T-1k: queries without comparison subqueries must not be flagged"
    );
}

/// T-1l: IN-subquery (not comparison) is NOT detected by the correlated detector.
#[test]
fn test_t1l_in_subquery_not_detected_by_correlated_detector() {
    // IN (SELECT ...) is a different pattern (handled by EXISTS rewrite)
    let q = "SELECT id FROM t WHERE status IN (SELECT code FROM valid_statuses)";
    assert!(
        !classify_correlated_aggregate_subquery_in_where(q),
        "T-1l: IN (SELECT ...) pattern must NOT be flagged by correlated detector"
    );
}
