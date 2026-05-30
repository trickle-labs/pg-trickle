//! GUC (Grand Unified Configuration) variables for pgtrickle.
//!
//! These are registered in `_PG_init()` and control the extension's behavior.
//! All GUC names are prefixed with `pg_trickle.`.

use pgrx::guc::*;

pub mod cdc;
pub mod dvm;
pub mod monitoring;
pub mod scheduler;

pub use cdc::*;
pub use dvm::*;
pub use monitoring::*;
pub use scheduler::*;

// ── Core GUC statics ──────────────────────────────────────────────────────

/// Master enable/disable switch for the extension.
pub static PGS_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Schema name for change buffer tables.
pub static PGS_CHANGE_BUFFER_SCHEMA: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"pgtrickle_changes"));

/// Maximum number of concurrent refresh workers.
///
/// Default: 4. A single worker typically sustains 200–500 refreshes/s on
/// an 8-core instance; 4 workers cover bursty diamond DAG parallelism
/// without saturating I/O.  Set equal to the number of parallel refresh
/// chains in your largest DAG for maximum throughput.
pub static PGS_MAX_CONCURRENT_REFRESHES: GucSetting<i32> = GucSetting::<i32>::new(4);

// ── Registration ──────────────────────────────────────────────────────────

/// Register all GUC variables for the pgtrickle extension.
pub fn register_gucs() {
    GucRegistry::define_bool_guc(
        c"pg_trickle.enabled",
        c"Master enable/disable switch for pgtrickle.",
        c"When false, the scheduler will not run and no refreshes will be triggered.",
        &PGS_ENABLED,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.change_buffer_schema",
        c"Schema name for change buffer tables.",
        c"CDC change data is stored in tables within this schema.",
        &PGS_CHANGE_BUFFER_SCHEMA,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.max_concurrent_refreshes",
        c"Maximum active refresh workers per database coordinator.",
        c"Limits the number of concurrent refresh operations within a single database. \
           In sequential mode (parallel_refresh_mode=off) this has no effect. \
           In parallel mode, the coordinator will not dispatch more than this many \
           concurrent refresh workers for one database.",
        &PGS_MAX_CONCURRENT_REFRESHES,
        1,  // min
        32, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    scheduler::register_scheduler_gucs();
    cdc::register_cdc_gucs();
    dvm::register_dvm_gucs();
    monitoring::register_monitoring_gucs();
}

// ── Core accessor functions ───────────────────────────────────────────────

/// Returns whether the pgtrickle extension is enabled.
pub fn pg_trickle_enabled() -> bool {
    PGS_ENABLED.get()
}

/// Returns the change buffer schema name.
pub fn pg_trickle_change_buffer_schema() -> String {
    PGS_CHANGE_BUFFER_SCHEMA
        .get()
        .map(|cs| cs.to_str().unwrap_or("pgtrickle_changes").to_string())
        .unwrap_or_else(|| "pgtrickle_changes".to_string())
}

/// Returns the maximum number of concurrent refresh workers.
pub fn pg_trickle_max_concurrent_refreshes() -> i32 {
    PGS_MAX_CONCURRENT_REFRESHES.get()
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::cdc::{
        normalize_cdc_trigger_mode, normalize_recursive_max_depth, threshold_mb_to_bytes,
    };
    use super::dvm::{
        normalize_diff_output_format, normalize_merge_join_strategy, normalize_merge_strategy,
        normalize_user_triggers_mode, normalize_volatile_function_policy,
    };
    use super::monitoring::{normalize_log_format, normalize_self_monitoring_auto_apply};
    use super::scheduler::{normalize_parallel_refresh_mode, normalize_refresh_strategy};
    use super::*;

    #[test]
    fn test_normalize_user_triggers_mode_defaults_to_auto() {
        assert_eq!(normalize_user_triggers_mode(None), UserTriggersMode::Auto);
        assert_eq!(
            normalize_user_triggers_mode(Some("auto".to_string())),
            UserTriggersMode::Auto
        );
        assert_eq!(
            normalize_user_triggers_mode(Some("on".to_string())),
            UserTriggersMode::Auto
        );
        assert_eq!(
            normalize_user_triggers_mode(Some("unexpected".to_string())),
            UserTriggersMode::Auto
        );
    }

    #[test]
    fn test_normalize_user_triggers_mode_accepts_off_case_insensitively() {
        assert_eq!(
            normalize_user_triggers_mode(Some("off".to_string())),
            UserTriggersMode::Off
        );
        assert_eq!(
            normalize_user_triggers_mode(Some("OFF".to_string())),
            UserTriggersMode::Off
        );
    }

    #[test]
    fn test_threshold_mb_to_bytes_converts_megabytes() {
        assert_eq!(threshold_mb_to_bytes(0), 0);
        assert_eq!(threshold_mb_to_bytes(100), 104_857_600);
        assert_eq!(threshold_mb_to_bytes(1024), 1_073_741_824);
    }

    #[test]
    fn test_normalize_cdc_trigger_mode_defaults_to_statement() {
        assert_eq!(normalize_cdc_trigger_mode(None), CdcTriggerMode::Statement);
        assert_eq!(
            normalize_cdc_trigger_mode(Some("statement".to_string())),
            CdcTriggerMode::Statement
        );
        assert_eq!(
            normalize_cdc_trigger_mode(Some("unexpected".to_string())),
            CdcTriggerMode::Statement
        );
    }

    #[test]
    fn test_normalize_cdc_trigger_mode_accepts_row_case_insensitively() {
        assert_eq!(
            normalize_cdc_trigger_mode(Some("row".to_string())),
            CdcTriggerMode::Row
        );
        assert_eq!(
            normalize_cdc_trigger_mode(Some("ROW".to_string())),
            CdcTriggerMode::Row
        );
    }

    #[test]
    fn test_normalize_recursive_max_depth_zero_disables_guard() {
        assert_eq!(normalize_recursive_max_depth(0), None);
        assert_eq!(normalize_recursive_max_depth(-5), None);
        assert_eq!(normalize_recursive_max_depth(100), Some(100));
    }

    #[test]
    fn test_parallel_refresh_mode_display_matches_as_str() {
        assert_eq!(ParallelRefreshMode::Off.as_str(), "off");
        assert_eq!(ParallelRefreshMode::DryRun.as_str(), "dry_run");
        assert_eq!(ParallelRefreshMode::On.as_str(), "on");
        assert_eq!(ParallelRefreshMode::DryRun.to_string(), "dry_run");
    }

    #[test]
    fn test_normalize_parallel_refresh_mode_defaults_to_on() {
        assert_eq!(
            normalize_parallel_refresh_mode(None),
            ParallelRefreshMode::On
        );
        assert_eq!(
            normalize_parallel_refresh_mode(Some("unexpected".to_string())),
            ParallelRefreshMode::On
        );
    }

    #[test]
    fn test_normalize_parallel_refresh_mode_accepts_supported_values() {
        assert_eq!(
            normalize_parallel_refresh_mode(Some("dry_run".to_string())),
            ParallelRefreshMode::DryRun
        );
        assert_eq!(
            normalize_parallel_refresh_mode(Some("DRY_RUN".to_string())),
            ParallelRefreshMode::DryRun
        );
        assert_eq!(
            normalize_parallel_refresh_mode(Some("on".to_string())),
            ParallelRefreshMode::On
        );
    }

    // ── P3: as_str coverage for all enum variants; threshold edge cases ─────

    #[test]
    fn test_user_triggers_mode_as_str() {
        assert_eq!(UserTriggersMode::Auto.as_str(), "auto");
        assert_eq!(UserTriggersMode::Off.as_str(), "off");
    }

    #[test]
    fn test_cdc_trigger_mode_as_str() {
        assert_eq!(CdcTriggerMode::Statement.as_str(), "statement");
        assert_eq!(CdcTriggerMode::Row.as_str(), "row");
    }

    #[test]
    fn test_parallel_refresh_mode_as_str_all_variants() {
        assert_eq!(ParallelRefreshMode::Off.as_str(), "off");
        assert_eq!(ParallelRefreshMode::DryRun.as_str(), "dry_run");
        assert_eq!(ParallelRefreshMode::On.as_str(), "on");
    }

    #[test]
    fn test_threshold_mb_to_bytes_negative_input_is_zero_or_negative() {
        // Negative megabytes should yield a non-positive byte count
        assert!(threshold_mb_to_bytes(-1) <= 0);
        assert!(threshold_mb_to_bytes(-100) < 0);
    }

    #[test]
    fn test_normalize_parallel_refresh_mode_case_insensitive_on() {
        assert_eq!(
            normalize_parallel_refresh_mode(Some("ON".to_string())),
            ParallelRefreshMode::On
        );
    }

    #[test]
    fn test_normalize_user_triggers_mode_roundtrip_via_as_str() {
        for (input, expected) in [
            ("off", UserTriggersMode::Off),
            ("OFF", UserTriggersMode::Off),
        ] {
            assert_eq!(
                normalize_user_triggers_mode(Some(input.to_string())),
                expected
            );
        }
        // as_str / normalize should be consistent
        assert_eq!(
            normalize_user_triggers_mode(Some(UserTriggersMode::Off.as_str().to_string())),
            UserTriggersMode::Off
        );
        assert_eq!(
            normalize_user_triggers_mode(Some(UserTriggersMode::Auto.as_str().to_string())),
            UserTriggersMode::Auto
        );
    }

    #[test]
    fn test_normalize_cdc_trigger_mode_roundtrip_via_as_str() {
        assert_eq!(
            normalize_cdc_trigger_mode(Some(CdcTriggerMode::Row.as_str().to_string())),
            CdcTriggerMode::Row
        );
        assert_eq!(
            normalize_cdc_trigger_mode(Some(CdcTriggerMode::Statement.as_str().to_string())),
            CdcTriggerMode::Statement
        );
    }

    #[test]
    fn test_normalize_volatile_function_policy_defaults_to_reject() {
        assert_eq!(
            normalize_volatile_function_policy(None),
            VolatileFunctionPolicy::Reject
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("reject".to_string())),
            VolatileFunctionPolicy::Reject
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("unexpected".to_string())),
            VolatileFunctionPolicy::Reject
        );
    }

    #[test]
    fn test_normalize_volatile_function_policy_accepts_warn_and_allow() {
        assert_eq!(
            normalize_volatile_function_policy(Some("warn".to_string())),
            VolatileFunctionPolicy::Warn
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("WARN".to_string())),
            VolatileFunctionPolicy::Warn
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("allow".to_string())),
            VolatileFunctionPolicy::Allow
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("ALLOW".to_string())),
            VolatileFunctionPolicy::Allow
        );
    }

    #[test]
    fn test_volatile_function_policy_as_str() {
        assert_eq!(VolatileFunctionPolicy::Reject.as_str(), "reject");
        assert_eq!(VolatileFunctionPolicy::Warn.as_str(), "warn");
        assert_eq!(VolatileFunctionPolicy::Allow.as_str(), "allow");
    }

    #[test]
    fn test_normalize_volatile_function_policy_roundtrip_via_as_str() {
        for policy in [
            VolatileFunctionPolicy::Reject,
            VolatileFunctionPolicy::Warn,
            VolatileFunctionPolicy::Allow,
        ] {
            assert_eq!(
                normalize_volatile_function_policy(Some(policy.as_str().to_string())),
                policy
            );
        }
    }

    #[test]
    fn test_normalize_merge_join_strategy_defaults_to_auto() {
        assert_eq!(normalize_merge_join_strategy(None), MergeJoinStrategy::Auto);
        assert_eq!(
            normalize_merge_join_strategy(Some("auto".to_string())),
            MergeJoinStrategy::Auto
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("unexpected".to_string())),
            MergeJoinStrategy::Auto
        );
    }

    #[test]
    fn test_normalize_merge_join_strategy_all_variants() {
        assert_eq!(
            normalize_merge_join_strategy(Some("hash_join".to_string())),
            MergeJoinStrategy::HashJoin
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("HASH_JOIN".to_string())),
            MergeJoinStrategy::HashJoin
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("nested_loop".to_string())),
            MergeJoinStrategy::NestedLoop
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("NESTED_LOOP".to_string())),
            MergeJoinStrategy::NestedLoop
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("merge_join".to_string())),
            MergeJoinStrategy::MergeJoin
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("MERGE_JOIN".to_string())),
            MergeJoinStrategy::MergeJoin
        );
    }

    #[test]
    fn test_merge_join_strategy_as_str() {
        assert_eq!(MergeJoinStrategy::Auto.as_str(), "auto");
        assert_eq!(MergeJoinStrategy::HashJoin.as_str(), "hash_join");
        assert_eq!(MergeJoinStrategy::NestedLoop.as_str(), "nested_loop");
        assert_eq!(MergeJoinStrategy::MergeJoin.as_str(), "merge_join");
    }

    #[test]
    fn test_normalize_merge_join_strategy_roundtrip_via_as_str() {
        for strategy in [
            MergeJoinStrategy::Auto,
            MergeJoinStrategy::HashJoin,
            MergeJoinStrategy::NestedLoop,
            MergeJoinStrategy::MergeJoin,
        ] {
            assert_eq!(
                normalize_merge_join_strategy(Some(strategy.as_str().to_string())),
                strategy
            );
        }
    }

    #[test]
    fn test_normalize_merge_strategy_defaults_to_auto() {
        assert_eq!(normalize_merge_strategy(None), MergeStrategy::Auto);
        assert_eq!(
            normalize_merge_strategy(Some("".to_string())),
            MergeStrategy::Auto
        );
        assert_eq!(
            normalize_merge_strategy(Some("garbage".to_string())),
            MergeStrategy::Auto
        );
    }

    #[test]
    fn test_normalize_merge_strategy_all_variants() {
        assert_eq!(
            normalize_merge_strategy(Some("merge".to_string())),
            MergeStrategy::Merge
        );
        // CORR-1: delete_insert now falls back to Auto with a warning
        assert_eq!(
            normalize_merge_strategy(Some("delete_insert".to_string())),
            MergeStrategy::Auto
        );
        assert_eq!(
            normalize_merge_strategy(Some("auto".to_string())),
            MergeStrategy::Auto
        );
        // Case-insensitive
        assert_eq!(
            normalize_merge_strategy(Some("DELETE_INSERT".to_string())),
            MergeStrategy::Auto
        );
        assert_eq!(
            normalize_merge_strategy(Some("MERGE".to_string())),
            MergeStrategy::Merge
        );
    }

    #[test]
    fn test_normalize_merge_strategy_roundtrip_via_as_str() {
        for strategy in [MergeStrategy::Auto, MergeStrategy::Merge] {
            assert_eq!(
                normalize_merge_strategy(Some(strategy.as_str().to_string())),
                strategy
            );
        }
    }

    // ── B-4: RefreshStrategy normalizer tests ───────────────────────

    #[test]
    fn test_normalize_refresh_strategy_defaults_to_auto() {
        assert_eq!(normalize_refresh_strategy(None), RefreshStrategy::Auto);
        assert_eq!(
            normalize_refresh_strategy(Some("auto".to_string())),
            RefreshStrategy::Auto
        );
        assert_eq!(
            normalize_refresh_strategy(Some("unexpected".to_string())),
            RefreshStrategy::Auto
        );
    }

    #[test]
    fn test_normalize_refresh_strategy_all_variants() {
        assert_eq!(
            normalize_refresh_strategy(Some("differential".to_string())),
            RefreshStrategy::Differential
        );
        assert_eq!(
            normalize_refresh_strategy(Some("DIFFERENTIAL".to_string())),
            RefreshStrategy::Differential
        );
        assert_eq!(
            normalize_refresh_strategy(Some("full".to_string())),
            RefreshStrategy::Full
        );
        assert_eq!(
            normalize_refresh_strategy(Some("FULL".to_string())),
            RefreshStrategy::Full
        );
    }

    #[test]
    fn test_refresh_strategy_as_str() {
        assert_eq!(RefreshStrategy::Auto.as_str(), "auto");
        assert_eq!(RefreshStrategy::Differential.as_str(), "differential");
        assert_eq!(RefreshStrategy::Full.as_str(), "full");
    }

    #[test]
    fn test_normalize_refresh_strategy_roundtrip_via_as_str() {
        for strategy in [
            RefreshStrategy::Auto,
            RefreshStrategy::Differential,
            RefreshStrategy::Full,
        ] {
            assert_eq!(
                normalize_refresh_strategy(Some(strategy.as_str().to_string())),
                strategy
            );
        }
    }

    // Note: GUC default value tests (PGS_WATERMARK_HOLDBACK_TIMEOUT,
    // PGS_SPILL_THRESHOLD_BLOCKS, PGS_SPILL_CONSECUTIVE_LIMIT) require a
    // PostgreSQL backend and are covered by E2E tests.  Calling
    // `GucSetting::get()` in multi-threaded unit tests triggers pgrx's
    // "postgres FFI may not be called from multiple threads" guard.

    // ── DF-G1: SelfMonitoringAutoApply normalizer tests ────────────────

    #[test]
    fn test_normalize_self_monitoring_auto_apply_defaults_to_off() {
        assert_eq!(
            normalize_self_monitoring_auto_apply(None),
            SelfMonitoringAutoApply::Off
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("off".to_string())),
            SelfMonitoringAutoApply::Off
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("unexpected".to_string())),
            SelfMonitoringAutoApply::Off
        );
    }

    #[test]
    fn test_normalize_self_monitoring_auto_apply_all_variants() {
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("threshold_only".to_string())),
            SelfMonitoringAutoApply::ThresholdOnly
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("THRESHOLD_ONLY".to_string())),
            SelfMonitoringAutoApply::ThresholdOnly
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("full".to_string())),
            SelfMonitoringAutoApply::Full
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("FULL".to_string())),
            SelfMonitoringAutoApply::Full
        );
    }

    #[test]
    fn test_self_monitoring_auto_apply_as_str() {
        assert_eq!(SelfMonitoringAutoApply::Off.as_str(), "off");
        assert_eq!(
            SelfMonitoringAutoApply::ThresholdOnly.as_str(),
            "threshold_only"
        );
        assert_eq!(SelfMonitoringAutoApply::Full.as_str(), "full");
    }

    #[test]
    fn test_normalize_self_monitoring_auto_apply_roundtrip() {
        for mode in [
            SelfMonitoringAutoApply::Off,
            SelfMonitoringAutoApply::ThresholdOnly,
            SelfMonitoringAutoApply::Full,
        ] {
            assert_eq!(
                normalize_self_monitoring_auto_apply(Some(mode.as_str().to_string())),
                mode
            );
        }
    }

    // ── v0.23.0: DiffOutputFormat normalizer tests ─────────────────

    #[test]
    fn test_normalize_diff_output_format_defaults_to_split() {
        assert_eq!(normalize_diff_output_format(None), DiffOutputFormat::Split);
        assert_eq!(
            normalize_diff_output_format(Some("split".to_string())),
            DiffOutputFormat::Split
        );
        assert_eq!(
            normalize_diff_output_format(Some("unexpected".to_string())),
            DiffOutputFormat::Split
        );
    }

    #[test]
    fn test_normalize_diff_output_format_accepts_merged() {
        assert_eq!(
            normalize_diff_output_format(Some("merged".to_string())),
            DiffOutputFormat::Merged
        );
        assert_eq!(
            normalize_diff_output_format(Some("MERGED".to_string())),
            DiffOutputFormat::Merged
        );
    }

    #[test]
    fn test_diff_output_format_as_str() {
        assert_eq!(DiffOutputFormat::Split.as_str(), "split");
        assert_eq!(DiffOutputFormat::Merged.as_str(), "merged");
    }

    #[test]
    fn test_normalize_diff_output_format_roundtrip() {
        for fmt in [DiffOutputFormat::Split, DiffOutputFormat::Merged] {
            assert_eq!(
                normalize_diff_output_format(Some(fmt.as_str().to_string())),
                fmt
            );
        }
    }

    // ── #536: FrontierHoldbackMode normalizer tests ──────────────────

    #[test]
    fn test_normalize_frontier_holdback_mode_defaults_to_xmin() {
        assert_eq!(
            normalize_frontier_holdback_mode(None),
            FrontierHoldbackMode::Xmin
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("xmin".to_string())),
            FrontierHoldbackMode::Xmin
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("XMIN".to_string())),
            FrontierHoldbackMode::Xmin
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("unexpected".to_string())),
            FrontierHoldbackMode::Xmin
        );
    }

    #[test]
    fn test_normalize_frontier_holdback_mode_none() {
        assert_eq!(
            normalize_frontier_holdback_mode(Some("none".to_string())),
            FrontierHoldbackMode::None
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("NONE".to_string())),
            FrontierHoldbackMode::None
        );
    }

    #[test]
    fn test_normalize_frontier_holdback_mode_lsn_bytes() {
        assert_eq!(
            normalize_frontier_holdback_mode(Some("lsn:1048576".to_string())),
            FrontierHoldbackMode::LsnBytes(1_048_576)
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("lsn:0".to_string())),
            FrontierHoldbackMode::LsnBytes(0)
        );
        // Invalid number → returns InvalidLsn sentinel (accessor converts to Xmin + warns)
        assert_eq!(
            normalize_frontier_holdback_mode(Some("lsn:notanumber".to_string())),
            FrontierHoldbackMode::InvalidLsn
        );
    }

    // ── v0.36.0: LogFormat normalizer tests ───────────────────────────────

    #[test]
    fn test_normalize_log_format_defaults_to_text() {
        assert_eq!(normalize_log_format(None), LogFormat::Text);
        assert_eq!(
            normalize_log_format(Some("text".to_string())),
            LogFormat::Text
        );
        assert_eq!(
            normalize_log_format(Some("unexpected".to_string())),
            LogFormat::Text
        );
    }

    #[test]
    fn test_normalize_log_format_accepts_json() {
        assert_eq!(
            normalize_log_format(Some("json".to_string())),
            LogFormat::Json
        );
        assert_eq!(
            normalize_log_format(Some("JSON".to_string())),
            LogFormat::Json
        );
    }

    #[test]
    fn test_log_format_as_str() {
        assert_eq!(LogFormat::Text.as_str(), "text");
        assert_eq!(LogFormat::Json.as_str(), "json");
    }

    // ── v0.36.0: ColumnarBackend normalizer tests ─────────────────────────

    #[test]
    fn test_normalize_columnar_backend_defaults_to_none() {
        assert_eq!(normalize_columnar_backend(None), ColumnarBackend::None);
        assert_eq!(
            normalize_columnar_backend(Some("none".to_string())),
            ColumnarBackend::None
        );
        assert_eq!(
            normalize_columnar_backend(Some("unexpected".to_string())),
            ColumnarBackend::None
        );
    }

    #[test]
    fn test_normalize_columnar_backend_all_variants() {
        assert_eq!(
            normalize_columnar_backend(Some("citus".to_string())),
            ColumnarBackend::Citus
        );
        assert_eq!(
            normalize_columnar_backend(Some("CITUS".to_string())),
            ColumnarBackend::Citus
        );
        // Removed variants default to None
        assert_eq!(
            normalize_columnar_backend(Some("pg_mooncake".to_string())),
            ColumnarBackend::None
        );
    }

    #[test]
    fn test_columnar_backend_is_append_only() {
        assert!(!ColumnarBackend::None.is_append_only());
        assert!(ColumnarBackend::Citus.is_append_only());
    }

    #[test]
    fn test_columnar_backend_as_str() {
        assert_eq!(ColumnarBackend::None.as_str(), "none");
        assert_eq!(ColumnarBackend::Citus.as_str(), "citus");
    }
}
