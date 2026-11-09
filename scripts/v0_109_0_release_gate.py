#!/usr/bin/env python3
"""Retain the prior release gates and require local WAL retirement qualification."""

import json

import check_ann_release_payload
import v0_108_3_release_gate as prior


CASES = {
    "e2e_wal_receipt_recovery_tests::" + name
    for name in (
        "test_last_wal_consumer_drop_rollback_preserves_slot_and_frontier",
        "test_deferred_consumer_registration_rollback_preserves_wal_slot",
        "test_slot_retirement_commits_before_drop_and_recovers_after_drop_crash",
        "test_slot_retirement_keeps_active_stale_and_mismatched_slots",
        "test_dropped_wal_source_retains_retirement_until_last_consumer_commits",
        "test_slot_adoption_rejects_removed_consumer_and_recovers_orphan_after_crash",
        "test_replacement_source_cannot_reassign_pending_slot_retirement",
        "test_slot_retirement_keeps_null_named_reference_across_source_oids",
    )
}


PAUSE_CASES = {
    "e2e_refresh_atomicity_tests::" + name
    for name in (
        "test_scheduler_pause_drain_waits_for_witnessed_inline_refresh",
        "test_scheduler_pause_drain_protects_transitive_immediate_descendant",
        "test_scheduler_pause_rejects_dispatch_queued_after_pause_returns",
        "test_scheduler_repeatable_read_job_rechecks_pause_before_refresh",
    )
}

PGVECTOR_CASES = {
    "e2e_pgvector_tests::" + name
    for name in (
        "test_centroid_randomized_refresh_churn_matches_native_avg",
        "test_centroid_crash_recovers_committed_source_and_rolls_back_refresh",
        "test_centroid_opt_in_applies_and_falls_back_after_corruption",
        "test_centroid_missing_state_rebuilds_or_stays_unbound",
        "test_centroid_repeated_large_rewrites_do_not_rebase_every_refresh",
        "test_centroid_unsafe_rebase_remains_unbound_without_retry_storm",
        "test_centroid_refresh_serializes_concurrent_source_writer",
        "test_pgvector_compat_installed_version",
        "test_pgvector_distance_types_and_functions_differential",
        "test_embedding_stream_table_v2_metrics_and_options",
        "test_embedding_stream_table_halfvec_and_sparsevec_indexes",
        "test_distance_subscription_typed_predicates_and_authorization",
        "test_distance_subscription_notifies_only_membership_crossings",
        "test_ann_worker_allows_reads_and_refresh_during_concurrent_reindex",
        "test_ann_worker_records_lock_timeout_and_can_retry",
        "test_ann_request_listing_filters_privileges_and_pages",
        "test_ann_supervisor_processes_authorized_request",
    )
} | {
    "e2e_bench_tests::pgvector_workload::centroid_benchmark_oracle_rejects_missing_components",
    "e2e_pgvector_hybrid_tests::test_tenant_tutorial_sql_runs_with_isolated_reader",
    "e2e_pgvector_revision_tests::test_revision_safe_generation_and_model_rollover",
}

PGVECTOR_UPGRADE_CASES = {
    "e2e_upgrade_tests::" + name for name in (
        "test_upgrade_embedding_helpers_from_01083",
        "test_upgrade_preserves_existing_vector_expression_index",
        "test_upgrade_distance_subscriptions_preserves_legacy_rows",
        "test_upgrade_ann_epoch_handles_old_catalog_and_rollback",
    )
}


def check_pgvector_contract(contract: dict) -> None:
    suites = {suite["id"]: suite for suite in contract["required_suites"]}
    workflow = (prior.ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    for version in ("0.8.1", "0.8.7"):
        suite = suites[f"pgvector-{version}"]
        cases = suite.get("required_cases", [])
        prior.shared.require(
            set(cases) == PGVECTOR_CASES and len(cases) == len(PGVECTOR_CASES)
            and suite.get("command_argv") == ["python3", "scripts/run_release_pgvector_suite.py", version]
            and suite.get("e2e_image") == f"pg_trickle_e2e_pgvector:{version}"
            and suite.get("runtime") is True and suite.get("requires_postgresql") is True
            and suite.get("nextest_retries") == 0,
            f"pgvector {version} must run every required case against the candidate package",
        )
        prior.shared.require(
            f"PGS_E2E_BASE_IMAGE=pg_trickle_release_candidate:local tests/build_e2e_pgvector_image.sh {version}" in workflow,
            f"pgvector {version} image must derive from the candidate package",
        )
    upgrade = suites["pgvector-catalog-upgrade"]
    cases = upgrade.get("required_cases", [])
    prior.shared.require(
        set(cases) == PGVECTOR_UPGRADE_CASES and len(cases) == len(PGVECTOR_UPGRADE_CASES)
        and upgrade.get("command_argv") == ["bash", "scripts/run_release_pgvector_upgrade.sh"]
        and upgrade.get("runtime") is True and upgrade.get("upgrade") is True
        and upgrade.get("requires_postgresql") is True and upgrade.get("nextest_retries") == 0,
        "pgvector catalog upgrade must run every required case against the candidate package",
    )


def check_pgvector_upgrade_script(script: str) -> None:
    for marker in (
        "pgvector/pgvector:0.8.7-pg18@sha256:c29f30688a9ad961691bdf0ec1650717cc132a5814619d80f80463d6e4c42e47",
        "pgtrickle.embedding_stream_table('upgrade_vector_st'",
        '"$VECTOR_AFTER" == "$VECTOR_BEFORE"',
        "bool_and(pgt_id IS NULL)",
        "SHOW pg_trickle.enable_centroid_state",
        "upgrade_vector_st_embedding_idx",
    ):
        prior.shared.require(marker in script, f"published vector upgrade check is missing {marker}")


def check_ann_packaging_workflow(workflow: str) -> None:
    tar_section = workflow.partition("name: Create archive (tar.gz)")[2].partition("name: Archive full SQL")[0]
    zip_section = workflow.partition("name: Create archive (zip)")[2].partition("name: Verify packaged ANN operator files")[0]
    for path in check_ann_release_payload.FILES:
        name = path.rsplit("/", 1)[-1]
        prior.shared.require(name in tar_section and name in zip_section, f"release archives must include {path}")
    prior.shared.require("run: python scripts/check_ann_release_payload.py" in workflow,
                         "release archives must verify the packaged ANN operator files")


def check_retirement_contract(contract: dict) -> None:
    release_gate = next(item for item in contract["required_suites"] if item["id"] == "release-gate")
    prior.shared.require(
        release_gate["command_argv"] == ["python3", "scripts/v0_109_0_release_gate.py"],
        "release qualification must invoke the current retirement gate",
    )
    suite = next(item for item in contract["required_suites"] if item["id"] == "wal-retirement")
    prior.shared.require(
        set(suite["required_cases"]) == CASES and len(suite["required_cases"]) == len(CASES)
        and suite["command_argv"] == [
            "bash", "scripts/run_light_e2e_tests.sh", "--no-capture", "--test",
            "e2e_wal_receipt_recovery_tests",
        ]
        and suite["e2e_image"] == "pg_trickle_release_candidate:local",
        "retirement qualification must execute all named cases against the release candidate",
    )
    upgrade = next(item for item in contract["required_suites"] if item["id"] == "wal-retirement-upgrade")
    prior.shared.require(
        upgrade["command_argv"] == ["bash", "scripts/qualify_live_package_upgrade.sh", "0.108.3", "0.109.0"],
        "retirement upgrade must start from the actual published predecessor package",
    )


def main() -> None:
    prior.VERSION = "0.109.0"
    prior.PREVIOUS_VERSION = "0.108.3"
    prior.SOURCE_VERSIONS = ["0.108.3", "0.106.1"]
    prior.GATE_SCRIPT = "v0_109_0_release_gate.py"
    prior.REQUIRED_CASES = prior.REQUIRED_CASES | CASES | PAUSE_CASES
    prior.REQUIRED_SHARDS = prior.REQUIRED_SHARDS | {"wal-retirement"}
    prior.EXPECTED_SUITES = prior.EXPECTED_SUITES | {
        "wal-retirement", "wal-retirement-upgrade", "pgvector-0.8.1", "pgvector-0.8.7", "pgvector-catalog-upgrade",
    }
    contract = json.loads((prior.ROOT / "tests/release/v0.109.0-qualification.json").read_text())
    check_retirement_contract(contract)
    check_pgvector_contract(contract)
    check_ann_packaging_workflow((prior.ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8"))
    check_pgvector_upgrade_script((prior.ROOT / "scripts/qualify_live_package_upgrade.sh").read_text(encoding="utf-8"))
    prior.main()


if __name__ == "__main__":
    main()
