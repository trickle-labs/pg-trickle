#!/usr/bin/env python3
"""Validate the v0.108.0 support and operator qualification contract."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import generate_capability_manifest as capabilities
import v0_106_1_release_gate as shared


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.108.0"
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


def main() -> None:
    shared.VERSION = VERSION
    shared.PREVIOUS_VERSION = "0.107.0"
    shared.EXPECTED_SOURCE_VERSIONS = ["0.106.1", "0.107.0"]
    shared.QUALIFICATION = ROOT / "tests/release/v0.108.0-qualification.json"
    shared.GATE_SCRIPT = "v0_108_0_release_gate.py"
    shared.EXPECTED_SUITES = EXPECTED_SUITES
    check_support_contract()
    shared.main()


if __name__ == "__main__":
    main()
