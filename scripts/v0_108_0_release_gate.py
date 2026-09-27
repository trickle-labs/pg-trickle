#!/usr/bin/env python3
"""Validate the v0.108.0 support and operator qualification contract."""

from __future__ import annotations

import argparse
import copy
import fnmatch
import json
import re
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


def missing_unsafe_asan_cases(output: str, required_cases: set[str]) -> list[str]:
    starts = list(re.finditer(r"^test (\S+) \.\.\.", output, re.MULTILINE))
    passed = set()
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(output)
        statuses = re.findall(
            r"^[ \t]*(ok|FAILED|ignored)[ \t]*$",
            output[start.end() : end],
            re.MULTILINE,
        )
        if statuses and statuses[-1] == "ok":
            passed.add(start.group(1).rsplit("::", 1)[-1])
    return sorted(case for case in required_cases if case.rsplit("::", 1)[-1] not in passed)


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
    instrumented = qualification.get("instrumented_qualification", {})
    expected_unsafe_cases = {
        "e2e_unsafe_boundary_tests::test_instrumented_pg18_boot_and_assertions_enabled",
        "e2e_unsafe_boundary_tests::test_instrumented_invalid_memory_probe_is_detected",
        "e2e_unsafe_boundary_tests::test_pipeline_copy_null_and_toasted_rows_survive_refresh",
        "e2e_unsafe_boundary_tests::test_owner_context_pg_error_restores_state_and_continues",
        "e2e_unsafe_boundary_tests::test_caller_context_pg_error_restores_state_and_continues",
        "e2e_unsafe_boundary_tests::test_dispatch_worker_cancel_cleanup_and_restart_recovers",
        "e2e_unsafe_boundary_tests::test_zz_instrumented_diagnostics_are_clean",
    }
    shared.require(
        instrumented.get("separate_from_exact_release_package") is True
        and instrumented.get("platform") == "linux/amd64"
        and instrumented.get("postgresql") == "18.6"
        and instrumented.get("assertions") == "--enable-cassert"
        and instrumented.get("instrumentation")
        == "AddressSanitizer on PostgreSQL and pg_trickle"
        and "--nocapture" in instrumented.get("test_command_argv", [])
        and set(instrumented.get("required_cases", [])) == expected_unsafe_cases,
        "unsafe boundary qualification is missing cases or is conflated with exact-release evidence",
    )
    cases = sorted(expected_unsafe_cases)
    first_case = cases[0]
    complete_output = "\n".join(
        f"test {case.rsplit('::', 1)[-1]} ... diagnostic output\nmore output\nok"
        if case == first_case
        else f"test {case.rsplit('::', 1)[-1]} ... ok"
        for case in cases
    )
    missing_output = "\n".join(
        f"test {case.rsplit('::', 1)[-1]} ... ok" for case in cases[1:]
    )
    shared.require(
        not missing_unsafe_asan_cases(complete_output, expected_unsafe_cases)
        and missing_unsafe_asan_cases(missing_output, expected_unsafe_cases) == [first_case]
        and missing_unsafe_asan_cases(
            complete_output.replace(
                "diagnostic output\nmore output\nok",
                "diagnostic output\nmore output\nFAILED",
                1,
            ),
            expected_unsafe_cases,
        )
        == [first_case],
        "unsafe boundary case-output check failed its pass, omission, or failure control",
    )
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
    for shard in qualification.get("required_shards", []):
        shared.require(
            suites[shard["suite_id"]].get("shard")
            == {key: shard[key] for key in ("id", "index", "count")},
            f"{shard['id']} suite metadata does not match release shard contract",
        )
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
    shared.require(
        "qualification_only:" in workflow
        and 'EXPECTED_TAG="v${VERSION}"' in workflow
        and workflow.count(
            "if: ${{ github.event_name != 'workflow_dispatch' || inputs.qualification_only != true }}"
        ) == 3
        and "Publication stage ended as $stage during qualification-only run." in workflow,
        "candidate qualification mode must bind the package version and skip every publisher",
    )
    shared.require(
        "Run separate unsafe-boundary ASan qualification" in workflow
        and "qualification-logs/unsafe-asan" in workflow
        and "steps.unsafe-asan.outcome" in workflow
        and "Require every declared instrumented case to pass" in workflow
        and "--unsafe-asan-log qualification-logs/unsafe-asan/workloads.log" in workflow,
        "release qualification does not require and retain a separate unsafe-boundary instrumented run",
    )
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
    unsafe_filter = ci.split("unsafe_boundary_changed:\n", 1)[1].split(
        "\n\n  unsafe-boundary-asan:", 1
    )[0]
    unsafe_patterns = re.findall(r"^\s+- '([^']+)'$", unsafe_filter, re.MULTILINE)
    for path in (
        "src/error.rs",
        "src/refresh/pipeline.rs",
        "src/api/security_context.rs",
        "src/scheduler/dispatch.rs",
        "tests/e2e/mod.rs",
        "tests/fixtures/asan_pg18_leak.supp",
        "tests/fixtures/initdb-asan-wrapper.sh",
    ):
        fixture_selected = any(fnmatch.fnmatchcase(path, pattern) for pattern in unsafe_patterns)
        shared.require(
            fixture_selected and "unsafe_boundary_changed" in ci,
            f"changed-file fixture {path} does not select unsafe boundary checks",
        )
        disabled_patterns = [pattern for pattern in unsafe_patterns if pattern != path]
        shared.require(
            not any(fnmatch.fnmatchcase(path, pattern) for pattern in disabled_patterns),
            f"disabled route negative control still selects {path}",
        )
    asan_dockerfile = (ROOT / "tests/Dockerfile.e2e-asan").read_text(encoding="utf-8")
    initdb_wrapper = (ROOT / "tests/fixtures/initdb-asan-wrapper.sh").read_text(encoding="utf-8")
    asan_suppressions = (ROOT / "tests/fixtures/asan_pg18_leak.supp").read_text(encoding="utf-8")
    shared.require(
        "ASAN_OPTIONS=halt_on_error=1:abort_on_error=1:detect_leaks=1" in asan_dockerfile
        and "LSAN_OPTIONS=suppressions=/etc/asan/asan_pg18_leak.supp:print_suppressions=1" in asan_dockerfile
        and initdb_wrapper.count("detect_leaks=0") == 1
        and "$(basename \"$0\").asan-helper" in initdb_wrapper
        and "for helper in initdb pg_ctl psql; do" in asan_dockerfile
        and "${helper}.asan-helper" in asan_dockerfile
        and "/usr/local/pgsql/bin/postgres" not in asan_dockerfile
        and "ASAN_OPTIONS=halt_on_error=1:abort_on_error=1:detect_leaks=0" in initdb_wrapper,
        "instrumented image must scope leak detection off to initdb, pg_ctl, psql, and the temporary bootstrap server, and retain it for workload servers",
    )
    suppression_entries = re.findall(r"^[ \t]*leak:\S+[ \t]*$", asan_suppressions, re.MULTILINE)
    shared.require(
        suppression_entries == ["leak:save_ps_display_args", "leak:do_ereport"]
        and "PostgreSQL 18.6" in asan_suppressions
        and "save_ps_display_args" in asan_suppressions
        and "pgrx-pg-sys 0.18.0" in asan_suppressions
        and "do_ereport" in asan_suppressions
        and "PostgreSQL ERROR longjmps through Rust" in asan_suppressions
        and len(re.findall(r"^[ \t]*leak:\S+[ \t]*$", asan_suppressions, re.MULTILINE)) == 2,
        "instrumented image must suppress only the documented PostgreSQL and pgrx 0.18 process-lifetime allocations",
    )
    shared.require(
        "unsafe-boundary-asan:" in ci
        and "needs.detect-e2e-gates.outputs.unsafe_boundary_changed == 'true'" in ci
        and "tests/e2e_unsafe_boundary_tests" in ci
        and "--test-threads=1 --nocapture" in ci
        and "--test-threads=1 --nocapture" in workflow,
        "unsafe boundary changes do not run their instrumented E2E slice",
    )
    shared.require(
        'echo "listen_addresses = \'*\'" >> /usr/local/pgsql/share/postgresql.conf.sample' in asan_dockerfile
        and 'listening on IPv4 address \\"0.0.0.0\\"' in (ROOT / "tests/e2e/mod.rs").read_text(encoding="utf-8"),
        "ASan E2E image does not expose its final PostgreSQL listener through Testcontainers port mapping",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--unsafe-asan-log",
        type=Path,
        help="verify every required instrumented test passed in this Cargo test log",
    )
    args = parser.parse_args()
    shared.VERSION = VERSION
    shared.PREVIOUS_VERSION = "0.107.0"
    shared.EXPECTED_SOURCE_VERSIONS = ["0.106.1", "0.107.0"]
    shared.QUALIFICATION = ROOT / "tests/release/v0.108.0-qualification.json"
    shared.GATE_SCRIPT = "v0_108_0_release_gate.py"
    shared.EXPECTED_SUITES = EXPECTED_SUITES
    check_support_contract()
    if args.unsafe_asan_log is not None:
        qualification = json.loads(
            (ROOT / "tests/release/v0.108.0-qualification.json").read_text(encoding="utf-8")
        )
        required_cases = set(qualification["instrumented_qualification"]["required_cases"])
        output = args.unsafe_asan_log.read_text(encoding="utf-8")
        missing = missing_unsafe_asan_cases(output, required_cases)
        shared.require(
            not missing,
            f"instrumented ASan cases missing or not passed: {missing}",
        )
    subprocess.run(
        [sys.executable, "-m", "unittest", "scripts.test_release_evidence"],
        cwd=ROOT,
        check=True,
    )
    shared.main()


if __name__ == "__main__":
    main()
