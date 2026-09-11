#!/usr/bin/env python3
"""Validate the v0.105.1 runtime qualification contract and release surfaces."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.105.1"
SOURCE_VERSION = "0.105.0"
QUALIFICATION = ROOT / "tests/release/v0.105.1-qualification.json"
EXPECTED_SUITES = {
    "graph-v1",
    "delta-v1",
    "dvm-oracle",
    "recovery",
    "wal-admission",
    "upgrade-chain",
    "version-sync",
    "package-smoke",
    "monitoring-contract",
    "release-artifact-smoke",
}
EXPECTED_ARTIFACTS = {"linux-amd64", "linux-arm64", "macos-arm64", "windows-amd64"}
EXACT_VERSION = re.compile(r"^\d+\.\d+\.\d+$")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"v0.105.1 release gate failed: {message}")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"v0.105.1 release gate failed: {path.relative_to(ROOT)}: {error}") from error
    require(isinstance(value, dict), f"{path.relative_to(ROOT)} must contain an object")
    return value


def reject_placeholders(value: Any, path: str) -> None:
    if value is None:
        require(False, f"{path} must not be null")
    elif isinstance(value, str):
        require(bool(value.strip()), f"{path} must not be empty")
        require("tbd" not in value.lower() and "*" not in value, f"{path} contains a placeholder")
        if path.endswith("version") or path.endswith("release_version"):
            require(EXACT_VERSION.fullmatch(value) is not None, f"{path} must be x.y.z")
    elif isinstance(value, dict):
        for key, child in value.items():
            reject_placeholders(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_placeholders(child, f"{path}[{index}]")


def require_test(path: Path, name: str) -> None:
    text = path.read_text(encoding="utf-8")
    require(re.search(rf"\bfn\s+{re.escape(name)}\b", text) is not None, f"missing test {name}")


def main() -> None:
    cargo = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    require(cargo["package"]["version"] == VERSION, "Cargo.toml version drift")

    lock = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
    require(re.search(rf'name = "pg_trickle"\s+version = "{VERSION}"', lock) is not None, "Cargo.lock version drift")

    meta = read_json(ROOT / "META.json")
    require(meta.get("version") == VERSION, "META.json top-level version drift")
    require(meta.get("provides", {}).get("pg_trickle", {}).get("version") == VERSION, "META.json provides version drift")

    manifest = read_json(ROOT / "docs/capability-manifest.json")
    require(manifest.get("release_version") == VERSION, "capability manifest version drift")

    qualification = read_json(QUALIFICATION)
    reject_placeholders(qualification, "qualification")
    require(qualification.get("contract_version") == 1, "qualification contract version drift")
    require(qualification.get("release_version") == VERSION, "qualification release version drift")
    require(qualification.get("source_versions") == [SOURCE_VERSION], "source version boundary drift")

    suites = qualification.get("required_suites")
    require(isinstance(suites, list), "required_suites must be a list")
    suite_ids = {suite.get("id") for suite in suites if isinstance(suite, dict)}
    require(suite_ids == EXPECTED_SUITES, "required suite set is incomplete")
    require(len(suite_ids) == len(suites), "required suite identifiers must be unique")
    for suite in suites:
        require(suite.get("required") is True and suite.get("enabled") is True, f"suite {suite.get('id')!r} is not enabled")
        require(suite.get("expected_status") == "passed", f"suite {suite.get('id')!r} must require passed evidence")
        for field in ("command", "suite_version", "workload"):
            require(isinstance(suite.get(field), str) and suite[field].strip(), f"suite {suite.get('id')!r} lacks {field}")

    deferred = {suite.get("id") for suite in qualification.get("deferred_suites", []) if isinstance(suite, dict)}
    require(deferred == {"soak-72h", "longevity-7d"}, "deferred long-running suites drifted")
    for suite in qualification["deferred_suites"]:
        require(suite.get("required") is False and suite.get("enabled") is False and suite.get("status") == "deferred", f"deferred suite {suite.get('id')!r} must remain deferred")

    artifacts = qualification.get("artifacts")
    require({item.get("id") for item in artifacts} == EXPECTED_ARTIFACTS, "artifact set is incomplete")
    require(all(item.get("version") == VERSION and item.get("postgresql_major") == 18 for item in artifacts), "artifact metadata drift")

    archive = ROOT / f"sql/archive/pg_trickle--{VERSION}.sql"
    migration = ROOT / f"sql/pg_trickle--{SOURCE_VERSION}--{VERSION}.sql"
    require(archive.is_file(), "full SQL archive missing")
    require(migration.is_file(), "upgrade migration missing")
    require(VERSION in migration.read_text(encoding="utf-8"), "upgrade migration does not record the release")

    for path in (ROOT / "tests/e2e_v104_conformance_tests.rs", ROOT / "tests/e2e_dvm_composition_tests.rs", ROOT / "tests/e2e_diff_full_equivalence_tests.rs", ROOT / "tests/e2e_failure_recovery_tests.rs", ROOT / "tests/e2e_publication_crash_recovery_tests.rs", ROOT / "tests/e2e_v098_stability_tests.rs", ROOT / "tests/e2e_upgrade_tests.rs"):
        require(path.is_file(), f"missing qualification test file {path.relative_to(ROOT)}")
    require_test(ROOT / "tests/e2e_v104_conformance_tests.rs", "test_v104_graph_reference_coordinator_commits_publication")
    require_test(ROOT / "tests/e2e_v104_conformance_tests.rs", "test_v104_delta_reference_consumer_reads_and_acknowledges")
    require_test(ROOT / "tests/e2e_v104_conformance_tests.rs", "test_v104_graph_source_owner_can_delegate_coordinator")
    require_test(ROOT / "tests/e2e_dvm_composition_tests.rs", "test_v0873_mandatory_composition_matrix")
    require_test(ROOT / "tests/e2e_diff_full_equivalence_tests.rs", "test_diff_full_equivalence_inner_join")
    require_test(ROOT / "tests/e2e_failure_recovery_tests.rs", "test_statement_timeout_during_refresh_recovers")
    require_test(ROOT / "tests/e2e_failure_recovery_tests.rs", "test_recovery_clone_isolation_requires_explicit_adoption")
    require_test(ROOT / "tests/e2e_publication_crash_recovery_tests.rs", "test_err4_publication_subscriber_catches_up_after_disruption")
    require_test(ROOT / "tests/e2e_v098_stability_tests.rs", "test_v103_wal_admission_is_receipt_backed")
    require_test(ROOT / "tests/e2e_upgrade_tests.rs", "test_upgrade_quiesce_preserves_pending_deltas")

    wal_tests = (ROOT / "tests/e2e_wal_cdc_tests.rs").read_text(encoding="utf-8")
    require("#![cfg(any())]" in wal_tests, "unsupported WAL polling tests must stay disabled")
    policy_tests = (ROOT / "src/refresh/tests.rs").read_text(encoding="utf-8")
    require("test_full_policy_rejects_whole_query_transition" in policy_tests, "full_policy ERROR coverage is missing")

    roadmap = (ROOT / "roadmap/v0.105.1.md").read_text(encoding="utf-8")
    require("> **Status:** Released" in roadmap, "roadmap release status is stale")
    full_details = (ROOT / "roadmap/v0.105.1.md-full.md").read_text(encoding="utf-8")
    require("Status: Released" in full_details, "full release details are missing released status")
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    require("[0.105.1]" in changelog, "CHANGELOG.md is missing the release entry")

    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    for marker in (
        "github.ref_name == 'v0.105.1'",
        "python3 scripts/v0_105_1_release_gate.py",
        "tests/release/v0.105.1-qualification.json",
        "--suite dvm-oracle=passed",
        "--suite recovery=passed",
        "--suite wal-admission=passed",
        "--suite upgrade-chain=passed",
        "e2e_dvm_composition_tests",
        "e2e_failure_recovery_tests",
        "e2e_publication_crash_recovery_tests",
    ):
        require(marker in workflow, f"release workflow is missing {marker}")

    subprocess.run([sys.executable, str(ROOT / "scripts/generate_capability_manifest.py"), "--check"], cwd=ROOT, check=True)
    subprocess.run([str(ROOT / "scripts/check_version_sync.sh")], cwd=ROOT, check=True)
    print("v0.105.1 release gate passed")


if __name__ == "__main__":
    main()
