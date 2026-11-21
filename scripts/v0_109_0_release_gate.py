#!/usr/bin/env python3
"""Retain prior release gates and require WAL retirement and partition retention evidence."""

import json
import re

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
        "test_pgvector_compat_installed_version",
        "test_pgvector_distance_types_and_functions_differential",
        "test_embedding_stream_table_v2_metrics_and_options",
        "test_embedding_stream_table_halfvec_and_sparsevec_indexes",
        "test_distance_subscription_typed_predicates_and_authorization",
        "test_distance_subscription_notifies_only_membership_crossings",
        "test_distance_subscription_duplicate_predicates_with_user_trigger",
        "test_distance_subscription_concurrent_commit_and_rollback",
        "test_distance_subscription_trigger_concurrent_commit_and_rollback",
        "test_ann_worker_allows_reads_and_refresh_during_concurrent_reindex",
        "test_ann_supervisor_allows_restricted_reads_and_writes_during_reindex",
        "test_ann_worker_records_lock_timeout_and_can_retry",
        "test_ann_worker_retries_after_restart_during_concurrent_reindex",
        "test_ann_request_listing_filters_privileges_and_pages",
        "test_ann_supervisor_processes_authorized_request",
        "test_ann_supervisor_reconnects_after_database_restart",
        "test_ann_supervisor_bounds_repeated_failures",
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

RETENTION_CASES = {
    "e2e_partition_tests::test_retention_trim_partitioned_stream_storage",
    "e2e_partition_tests::test_retention_concurrent_detach_preserves_retained_history",
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
    names = sorted(case.split("::", 1)[1] for case in CASES)
    selector = "test(/^(" + "|".join(names) + ")$/)"
    prior.shared.require(
        set(suite["required_cases"]) == CASES and len(suite["required_cases"]) == len(CASES)
        and suite["command_argv"] == [
            "bash", "scripts/run_e2e_tests.sh", "--test-threads", "1", "--test",
            "e2e_wal_receipt_recovery_tests", "-E", selector,
        ]
        and suite["e2e_image"] == "pg_trickle_release_candidate:local",
        "retirement qualification must execute all named cases against the release candidate",
    )
    upgrade = next(item for item in contract["required_suites"] if item["id"] == "wal-retirement-upgrade")
    prior.shared.require(
        upgrade["command_argv"] == ["bash", "scripts/qualify_live_package_upgrade.sh", "0.108.3", "0.109.0"],
        "retirement upgrade must start from the actual published predecessor package",
    )
    recovery = next(item for item in contract["required_suites"] if item["id"] == "recovery")
    args = recovery["command_argv"]
    binaries = {args[index + 1] for index, arg in enumerate(args[:-1]) if arg == "--test"}
    prior.shared.require(
        {case.split("::", 1)[0] for case in recovery["required_cases"]} <= binaries,
        "recovery suite must select every required test binary",
    )
    prior.shared.require("-E" in args and args.index("-E") + 1 < len(args), "recovery suite must select its required cases")
    expression = args[args.index("-E") + 1]
    prior.shared.require(expression.startswith("test(/") and expression.endswith("/)"), "recovery selector must be a test regex")
    pattern = re.compile(expression[6:-2])
    prior.shared.require(
        all(re.fullmatch(pattern, case.split("::", 1)[1]) for case in recovery["required_cases"]),
        "recovery selector must include every required case",
    )


def check_subscription_scale_contract(contract: dict) -> None:
    prior.shared.require(
        not any(suite.get("id") == "subscription-scale-1m" for suite in contract["required_suites"]),
        "the 1M source-row subscription suite is outside v0.109.0 scope",
    )
    suite = next(
        (item for item in contract["required_suites"] if item["id"] == "subscription-scale-100k"),
        None,
    )
    prior.shared.require(suite is not None, "the exact-package subscription matrix through 100,000 rows is required")
    if suite is None:
        return
    prior.shared.require(
        suite.get("kind") == "measurement"
        and suite.get("artifact_id") == "linux-amd64"
        and suite.get("required") is True
        and suite.get("runtime") is True
        and suite.get("upgrade") is False
        and suite.get("requires_postgresql") is True
        and suite.get("suite_version") == "0.109.0"
        and suite.get("command_argv") == ["python3", "scripts/run_release_subscription_scale.py"]
        and suite.get("measurement_file") == "qualification-logs/raw/subscription-scale-100k.json"
        and suite.get("e2e_image") == "pg_trickle_e2e_pgvector:0.8.7"
        and suite.get("nextest_retries") == 0
        and suite.get("workload")
        == "exact-package 384-dimensional 1k/10k/100k source-row subscription matrix with 100k-row plan controls; not a RAG benchmark",
        "subscription scale must measure the exact release package separately from RAG",
    )


def check_retention_contract(contract: dict) -> None:
    suite = next(item for item in contract["required_suites"] if item["id"] == "partition-retention")
    names = sorted(case.split("::", 1)[1] for case in RETENTION_CASES)
    selector = "test(/^({})$/)".format("|".join(names))
    prior.shared.require(
        set(suite["required_cases"]) == RETENTION_CASES
        and len(suite["required_cases"]) == len(RETENTION_CASES)
        and suite["command_argv"] == [
            "bash", "scripts/run_e2e_tests.sh", "--test", "e2e_partition_tests",
            "-E", selector,
        ]
        and suite["e2e_image"] == "pg_trickle_release_candidate:local"
        and suite["runtime"] is True
        and suite["upgrade"] is False,
        "partition-retention qualification must execute the TRIM and RETAIN cases against the release candidate",
    )
    script = (prior.ROOT / "scripts/qualify_live_package_upgrade.sh").read_text(encoding="utf-8")
    for marker in (
        "to_regprocedure('pgtrickle.set_stream_table_partition_retention(text,text,text,text)')",
        "to_regprocedure('pgtrickle.get_stream_table_partition_retention(text)')",
        "to_regprocedure('pgtrickle.create_stream_table_with_retention(text,text,text,text,text,text)')",
        "evtname='pg_trickle_ddl_start_tracker'",
    ):
        prior.shared.require(marker in script, f"published upgrade check is missing retention marker {marker}")


def main() -> None:
    prior.VERSION = "0.109.0"
    prior.PREVIOUS_VERSION = "0.108.3"
    prior.SOURCE_VERSIONS = ["0.108.3", "0.106.1"]
    prior.GATE_SCRIPT = "v0_109_0_release_gate.py"
    prior.REQUIRED_CASES = prior.REQUIRED_CASES | CASES | PAUSE_CASES
    prior.REQUIRED_SHARDS = prior.REQUIRED_SHARDS | {"wal-retirement"}
    prior.EXPECTED_SUITES = prior.EXPECTED_SUITES | {
        "wal-retirement", "wal-retirement-upgrade", "partition-retention",
        "pgvector-0.8.1", "pgvector-0.8.7", "pgvector-catalog-upgrade", "subscription-scale-100k",
    }
    contract = json.loads((prior.ROOT / "tests/release/v0.109.0-qualification.json").read_text())
    check_retirement_contract(contract)
    check_retention_contract(contract)
    check_pgvector_contract(contract)
    check_ann_packaging_workflow((prior.ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8"))
    check_subscription_scale_contract(contract)
    check_pgvector_upgrade_script((prior.ROOT / "scripts/qualify_live_package_upgrade.sh").read_text(encoding="utf-8"))
    prior.main()


if __name__ == "__main__":
    main()
