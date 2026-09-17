#!/usr/bin/env python3
"""Validate the v0.107.0 support and operator qualification contract."""

from __future__ import annotations

import copy
from pathlib import Path

import generate_capability_manifest as capabilities
import v0_106_1_release_gate as shared


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.107.0"
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
}


def check_support_contract() -> None:
    source = capabilities.read_source()
    capabilities.validate(source)
    statuses = {item["status"] for item in source["capabilities"]}
    shared.require(
        statuses == {"stable", "experimental", "unavailable", "future"},
        "support lifecycle statuses are incomplete",
    )
    shared.require(
        all(item.get("test") and item.get("effective_strategy") for item in source["examples"]),
        "query-family fixtures must bind results to runtime tests",
    )

    broken = copy.deepcopy(source)
    broken["examples"][0]["test"] = "missing_runtime_fixture"
    try:
        capabilities.validate(broken)
    except ValueError:
        pass
    else:
        raise SystemExit("v0.107.0 release gate failed: missing runtime fixture was accepted")

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    config = (ROOT / "docs/CONFIGURATION.md").read_text(encoding="utf-8")
    shared.require("auto` is an alias for `trigger" not in readme, "README retains stale auto CDC claim")
    shared.require("Rejected with `PGT_EXT_CDC_UNAVAILABLE`" not in config, "configuration retains stale WAL claim")


def main() -> None:
    shared.VERSION = VERSION
    shared.PREVIOUS_VERSION = "0.106.1"
    shared.QUALIFICATION = ROOT / "tests/release/v0.107.0-qualification.json"
    shared.GATE_SCRIPT = "v0_107_0_release_gate.py"
    shared.EXPECTED_SUITES = EXPECTED_SUITES
    check_support_contract()
    shared.main()


if __name__ == "__main__":
    main()
