#!/usr/bin/env python3
"""Validate the v0.108.0 support and operator qualification contract."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import generate_capability_manifest as capabilities
import v0_106_1_release_gate as shared


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.108.0"
REQUIRED_CASES = {
    "e2e_sensitivity_baseline_tests::test_oracle_detects_same_count_different_content",
    "e2e_sensitivity_baseline_tests::test_oracle_detects_duplicate_multiplicity_mismatch",
    "e2e_sensitivity_baseline_tests::test_oracle_detects_schema_mismatch",
    "e2e_sensitivity_baseline_tests::test_oracle_detects_missing_user_column",
    "e2e_failure_recovery_tests::test_lock_timeout_during_refresh",
    "e2e_failure_recovery_tests::test_statement_timeout_during_refresh_recovers",
    "e2e_failure_recovery_tests::test_cancel_backend_during_refresh_recovers",
    "e2e_dvm_failpoint_tests::test_dvm_failpoint_preserves_last_committed_result_and_recovers",
    "e2e_refresh_atomicity_tests::test_refresh_after_apply_failure_rolls_back",
    "e2e_refresh_atomicity_tests::test_refresh_finalization_failure_rolls_back",
    "e2e_refresh_atomicity_tests::test_refresh_caller_rollback_preserves_committed_state",
    "e2e_refresh_atomicity_tests::test_refresh_savepoint_rollback_preserves_outer_transaction",
    "e2e_refresh_atomicity_tests::test_refresh_two_callers_publish_one_consistent_result",
    "e2e_refresh_atomicity_tests::test_refresh_concurrent_writer_preserves_next_batch",
    "e2e_refresh_atomicity_tests::test_refresh_auxiliary_state_failure_rolls_back",
    "e2e_refresh_atomicity_tests::test_refresh_partial_finalization_control_is_detected",
    "e2e_refresh_atomicity_tests::test_refresh_early_progress_control_is_detected",
    "e2e_publication_crash_recovery_tests::test_publication_recovery_initial_sync_dml_and_setup_failures",
    "e2e_publication_crash_recovery_tests::test_publication_recovery_subscriber_restart_catches_up",
    "e2e_publication_crash_recovery_tests::test_publication_recovery_publisher_crash_retains_slot_and_catches_up",
    "e2e_publication_crash_recovery_tests::test_publication_recovery_interrupted_catchup_and_negative_controls",
}
EXPECTED_SUITES = shared.EXPECTED_SUITES | {
    "support-contract",
    "query-family-fixtures",
    "auto-wal-transition",
    "composition",
    "semantic-negative-control",
    "operator-route-a",
    "operator-route-b",
    "publication-recovery",
    "resource-boundaries",
    "delta-qualification-default-off",
    "sensitivity-baseline",
}


def check_support_contract() -> None:
    source = capabilities.read_source()
    capabilities.validate(source)
    by_name = {item["capability"]: item for item in source["capabilities"]}
    graph = by_name["external_graph_refresh"]
    delta = by_name["output_delta_consumer"]
    shared.require(
        (graph["major_version"], graph["minor_version"]) == (1, 2),
        "Graph V1.2 capability is missing",
    )
    shared.require(
        graph.get("details", {}).get("differential_features")
        == [
            "stable_row_identity_encoder_v2",
            "custom_table_srf_out_columns",
            "lateral_immutable_composite_function",
        ],
        "Graph V1.2 differential feature identifiers drifted",
    )
    shared.require(
        (delta["major_version"], delta["minor_version"]) == (1, 1),
        "Delta V1.1 capability is missing",
    )
    shared.require(
        delta.get("details", {}).get("consumer_recovery_version") == 1
        and delta["details"].get("public_resnapshot_request") is True
        and delta["details"].get("public_consumer_validation") is True
        and delta["details"].get("qualification_api")
        == "qualify_output_delta_recovery",
        "Delta V1.1 recovery details drifted",
    )
    statuses = {item["status"] for item in source["capabilities"]}
    shared.require(
        statuses == {"stable", "experimental", "unavailable", "future"},
        "support lifecycle statuses are incomplete",
    )
    shared.require(
        all(item.get("test") and item.get("effective_strategy") for item in source["examples"]),
        "query-family fixtures must bind results to runtime tests",
    )
    examples = {item["id"]: item for item in source["examples"]}
    exact_evidence = examples.get("graph-v1-2-pg-mdm-compiler-v9-exact-evidence", {})
    immutable_equivalent = examples.get(
        "graph-v1-2-pg-mdm-compiler-v9-immutable-equivalent", {}
    )
    shared.require(
        exact_evidence.get("outcome") == "rejected"
        and exact_evidence.get("effective_strategy") == "UNAVAILABLE"
        and exact_evidence.get("reason_code") == "UNSUPPORTED_OPERATOR",
        "exact compiler-v9 concat_ws evidence must remain a named negative control",
    )
    shared.require(
        immutable_equivalent.get("outcome") == "accepted"
        and immutable_equivalent.get("effective_strategy") == "DIFFERENTIAL"
        and immutable_equivalent.get("reason_code") is None,
        "compiler-v9 immutable-equivalent evidence must remain distinct from exact SQL",
    )

    broken = copy.deepcopy(source)
    broken["examples"][0]["test"] = "missing_runtime_fixture"
    try:
        capabilities.validate(broken)
    except ValueError:
        pass
    else:
        raise SystemExit("v0.108.0 release gate failed: missing runtime fixture was accepted")

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    config = (ROOT / "docs/CONFIGURATION.md").read_text(encoding="utf-8")
    shared.require("auto` is an alias for `trigger" not in readme, "README retains stale auto CDC claim")
    shared.require("Rejected with `PGT_EXT_CDC_UNAVAILABLE`" not in config, "configuration retains stale WAL claim")

    qualification = json.loads(
        (ROOT / "tests/release/v0.108.0-qualification.json").read_text(encoding="utf-8")
    )
    shared.require(qualification.get("contract_version") == 3, "release qualification contract is not evidence-complete")
    shared.require(qualification.get("build_kind") == "exact-release", "qualification does not require exact release builds")
    shared.require(qualification.get("feature_scope") == "default-release", "qualification feature scope drifted")
    shared.require(qualification.get("candidate_identity", {}).get("required") is True, "candidate identity is optional")
    suites = {suite["id"]: suite for suite in qualification["required_suites"]}
    for suite_id, test_binary in (
        ("delta-v1", "e2e_v108_conformance_tests"),
        ("delta-qualification-default-off", "e2e_output_delta_qualification_disabled_tests"),
    ):
        suite = suites[suite_id]
        shared.require(
            suite.get("e2e_image") == "pg_trickle_release_candidate:local"
            and test_binary in suite.get("command_argv", []),
            f"{suite_id} does not run its v0.108 packaged conformance binary",
        )
    publication_suite = suites["publication-recovery"]
    light_runner = (ROOT / "scripts/run_light_e2e_tests.sh").read_text(encoding="utf-8")
    machine_format_runner = light_runner.split(
        'if [[ "${PGT_RELEASE_MACHINE_FORMAT:-0}" == "1" ]]; then', 1
    )[1].split("\nelif ", 1)[0]
    shared.require(
        'capture_args+=(--no-capture)' in machine_format_runner,
        "release machine-format no-capture suites must use Nextest serial mode",
    )
    publication_cases = {
        "e2e_publication_crash_recovery_tests::test_publication_recovery_initial_sync_dml_and_setup_failures",
        "e2e_publication_crash_recovery_tests::test_publication_recovery_subscriber_restart_catches_up",
        "e2e_publication_crash_recovery_tests::test_publication_recovery_publisher_crash_retains_slot_and_catches_up",
        "e2e_publication_crash_recovery_tests::test_publication_recovery_interrupted_catchup_and_negative_controls",
    }
    shared.require(
        publication_suite.get("e2e_image") == "pg_trickle_release_candidate:local"
        and publication_suite.get("command_argv")
        == ["bash", "scripts/run_light_e2e_tests.sh", "--no-capture", "--test", "e2e_publication_crash_recovery_tests"]
        and set(publication_suite.get("required_cases", [])) == publication_cases
        and len(publication_suite.get("required_cases", [])) == len(publication_cases)
        and publication_suite.get("shard") == {"id": "publication-topology", "index": 3, "count": 3},
        "publication recovery does not run serially against the exact candidate package",
    )
    shared.require(
        suites["graph-v1"].get("e2e_image") == "pg_trickle_release_candidate:local"
        and suites["graph-v1"].get("command_argv")
        == ["bash", "scripts/run_v108_conformance.sh"],
        "graph-v1 does not run the combined packaged v0.108 conformance suite",
    )
    shared.require(
        suites["pending-data-upgrade"]["command_argv"][-2:]
        == ["0.106.1", "0.108.0"],
        "live package upgrade does not cover the pg-mdm-pinned v0.106.1 package",
    )
    shared.require(
        suites["upgrade-chain"]["command_argv"][-2:] == ["0.107.0", "0.108.0"],
        "archive and upgrade completeness do not cover the release boundary",
    )
    shards = {shard["id"]: shard for shard in qualification.get("required_shards", [])}
    shared.require(
        set(qualification.get("required_cases", [])) == REQUIRED_CASES,
        "required release case identities are incomplete or renamed",
    )
    shared.require(
        set(shards) == {"sensitivity-baseline", "failure-recovery", "publication-topology"}
        and {case for shard in shards.values() for case in shard["required_cases"]} == REQUIRED_CASES
        and all(len(shard["required_cases"]) == len(set(shard["required_cases"])) for shard in shards.values()),
        "required release case shards are incomplete or ambiguous",
    )
    shared.require(
        set(suites["sensitivity-baseline"].get("required_cases", []))
        == set(shards["sensitivity-baseline"]["required_cases"])
        and set(suites["recovery"].get("required_cases", []))
        == set(shards["failure-recovery"]["required_cases"])
        and set(publication_suite.get("required_cases", []))
        == set(shards["publication-topology"]["required_cases"]),
        "required case suites do not match their shards",
    )
    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    shared.require("release-candidate.json" in workflow, "release archives do not carry candidate identity")
    stager = (ROOT / "scripts/stage_release_assets.py").read_text(encoding="utf-8")
    shared.require(
        'manifest.get("status") != "passed"' in stager,
        "publication does not revalidate passed release evidence",
    )
    shared.require(
        "scripts/stage_release_assets.py" in workflow,
        "publication does not stage and verify release evidence assets",
    )
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    shared.require("release-qualification-contract" in ci, "PR CI does not run the release evidence contract gate")
    shared.require(
        ".github/workflows/release.yml" in ci and "tests/release/**" in ci,
        "PR CI does not trigger for release contract changes",
    )


def main() -> None:
    shared.VERSION = VERSION
    shared.PREVIOUS_VERSION = "0.107.0"
    shared.EXPECTED_SOURCE_VERSIONS = ["0.106.1", "0.107.0"]
    shared.QUALIFICATION = ROOT / "tests/release/v0.108.0-qualification.json"
    shared.GATE_SCRIPT = "v0_108_0_release_gate.py"
    shared.EXPECTED_SUITES = EXPECTED_SUITES
    check_support_contract()
    subprocess.run(
        [sys.executable, "-m", "unittest", "scripts.test_release_evidence"],
        cwd=ROOT,
        check=True,
    )
    shared.main()


if __name__ == "__main__":
    main()
