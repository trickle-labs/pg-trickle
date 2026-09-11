#!/usr/bin/env python3
"""Validate the v0.105.0 qualification contract and release surfaces."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.105.0"
EXACT_VERSION = re.compile(r"^\d+\.\d+\.\d+$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
RESULT_STATES = {"passed", "failed", "skipped", "unavailable", "stale", "historical"}
REQUIRED_SUITES = {
    "graph-v1",
    "delta-v1",
    "version-sync",
    "package-smoke",
    "monitoring-contract",
    "release-artifact-smoke",
}
EXPECTED_ARTIFACTS = {"linux-amd64", "linux-arm64", "macos-arm64", "windows-amd64"}


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def reject_placeholders(value: Any, path: str, errors: list[str]) -> None:
    if value is None:
        fail(errors, f"{path} must not be null")
    elif isinstance(value, str):
        if not value.strip():
            fail(errors, f"{path} must not be empty")
        if "tbd" in value.lower() or "*" in value:
            fail(errors, f"{path} contains a placeholder or wildcard")
        if path.endswith("version") or path.endswith("release_version"):
            if not EXACT_VERSION.fullmatch(value):
                fail(errors, f"{path} must be an exact x.y.z version")
    elif isinstance(value, dict):
        for key, child in value.items():
            reject_placeholders(child, f"{path}.{key}", errors)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_placeholders(child, f"{path}[{index}]", errors)


def read_json(path: Path, errors: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(errors, f"{path.relative_to(ROOT)} is not valid JSON: {error}")
        return {}
    if not isinstance(value, dict):
        fail(errors, f"{path.relative_to(ROOT)} must contain an object")
        return {}
    return value


def main() -> int:
    errors: list[str] = []

    try:
        cargo = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        cargo_version = cargo["package"]["version"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        fail(errors, f"Cargo.toml cannot provide the package version: {error}")
        cargo_version = None
    if cargo_version != VERSION:
        fail(errors, "Cargo.toml version drift")

    lock = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
    if re.search(r'name = "pg_trickle"\s+version = "0\.105\.0"', lock) is None:
        fail(errors, "Cargo.lock version drift")

    meta = read_json(ROOT / "META.json", errors)
    if meta.get("version") != VERSION or meta.get("provides", {}).get("pg_trickle", {}).get("version") != VERSION:
        fail(errors, "META.json version drift")

    capability_manifest = read_json(ROOT / "docs/capability-manifest.json", errors)
    if capability_manifest.get("release_version") != VERSION:
        fail(errors, "capability manifest release version drift")

    qualification_path = ROOT / "tests/release/v0.105.0-qualification.json"
    qualification = read_json(qualification_path, errors)
    reject_placeholders(qualification, "qualification", errors)
    if qualification.get("contract_version") != 1:
        fail(errors, "contract_version must be 1")
    if qualification.get("release_version") != VERSION:
        fail(errors, f"qualification release_version must be {VERSION}")
    if qualification.get("source_versions") != ["0.104.0"]:
        fail(errors, "source_versions must contain exactly 0.104.0")

    required_suites = qualification.get("required_suites")
    suite_ids: list[str] = []
    if not isinstance(required_suites, list) or not required_suites:
        fail(errors, "required_suites must be non-empty")
        required_suites = []
    for index, suite in enumerate(required_suites):
        if not isinstance(suite, dict):
            fail(errors, f"required_suites[{index}] must be an object")
            continue
        suite_id = suite.get("id")
        if isinstance(suite_id, str):
            suite_ids.append(suite_id)
        if not isinstance(suite_id, str) or not suite_id:
            fail(errors, f"required_suites[{index}].id is missing")
        if suite.get("required") is not True or suite.get("enabled") is not True:
            fail(errors, f"suite {suite_id!r} must be required and enabled")
        if suite.get("expected_status") != "passed":
            fail(errors, f"suite {suite_id!r} must require passed evidence")
        for field in ("command", "suite_version", "workload"):
            if not isinstance(suite.get(field), str) or not suite[field].strip():
                fail(errors, f"suite {suite_id!r} is missing {field}")
    if set(suite_ids) != REQUIRED_SUITES:
        fail(errors, f"required suite set must be {sorted(REQUIRED_SUITES)}")
    if len(suite_ids) != len(set(suite_ids)):
        fail(errors, "required_suites contains duplicate identifiers")

    deferred_suites = qualification.get("deferred_suites")
    deferred_ids: list[str] = []
    if not isinstance(deferred_suites, list) or not deferred_suites:
        fail(errors, "deferred_suites must be non-empty")
        deferred_suites = []
    for index, suite in enumerate(deferred_suites):
        if not isinstance(suite, dict):
            fail(errors, f"deferred_suites[{index}] must be an object")
            continue
        suite_id = suite.get("id")
        if isinstance(suite_id, str):
            deferred_ids.append(suite_id)
        if suite.get("required") is not False or suite.get("enabled") is not False:
            fail(errors, f"deferred suite {suite_id!r} must be disabled and non-required")
        if suite.get("status") != "deferred":
            fail(errors, f"deferred suite {suite_id!r} must have status deferred")
    if set(deferred_ids) != {"soak-72h", "longevity-7d"}:
        fail(errors, "72-hour soak and seven-day longevity must be explicitly deferred")

    artifacts = qualification.get("artifacts")
    artifact_ids: list[str] = []
    if not isinstance(artifacts, list) or not artifacts:
        fail(errors, "artifacts must be non-empty")
        artifacts = []
    for index, artifact in enumerate(artifacts):
        if not isinstance(artifact, dict):
            fail(errors, f"artifacts[{index}] must be an object")
            continue
        artifact_id = artifact.get("id")
        if isinstance(artifact_id, str):
            artifact_ids.append(artifact_id)
        if artifact.get("version") != VERSION or artifact.get("postgresql_major") != 18:
            fail(errors, f"artifact {artifact_id!r} has incorrect version or PostgreSQL major")
    if set(artifact_ids) != EXPECTED_ARTIFACTS:
        fail(errors, f"artifact set must be {sorted(EXPECTED_ARTIFACTS)}")
    if len(artifact_ids) != len(set(artifact_ids)):
        fail(errors, "artifacts contains duplicate identifiers")

    budgets = qualification.get("performance_budgets")
    if not isinstance(budgets, list) or not budgets:
        fail(errors, "performance_budgets must be non-empty")
    else:
        for index, budget in enumerate(budgets):
            if not isinstance(budget, dict) or not isinstance(budget.get("threshold"), (int, float)):
                fail(errors, f"performance_budgets[{index}] must define a numeric threshold")

    evidence = qualification.get("evidence")
    required_fields = {
        "candidate_commit",
        "artifact_digest",
        "postgresql_version",
        "suite_version",
        "workload",
        "retained_logs",
    }
    if not isinstance(evidence, dict) or evidence.get("schema_version") != 2:
        fail(errors, "evidence must declare schema_version 2")
    elif set(evidence.get("required_fields", [])) != required_fields:
        fail(errors, "evidence required_fields are incomplete")
    elif set(evidence.get("allowed_result_states", [])) != RESULT_STATES:
        fail(errors, "evidence allowed_result_states are incomplete")

    previous_evidence = read_json(ROOT / "tests/release/v0.104-evidence.json", errors)
    if previous_evidence.get("release_version") != "0.104.0" or previous_evidence.get("status") != "historical":
        fail(errors, "v0.104 evidence record is not historical")
    if not COMMIT.fullmatch(previous_evidence.get("candidate_commit", "")):
        fail(errors, "v0.104 evidence record must identify its candidate commit")
    if {item.get("name") for item in previous_evidence.get("executed_suites", [])} != {"graph-v1", "delta-v1"}:
        fail(errors, "v0.104 evidence record must name both conformance suites")

    roadmap = (ROOT / "roadmap/v0.105.0.md").read_text(encoding="utf-8")
    if "> **Status:** Released" not in roadmap:
        fail(errors, "v0.105.0 roadmap status is stale")
    full_details = (ROOT / "roadmap/v0.105.0.md-full.md").read_text(encoding="utf-8")
    if full_details.count("✅ Done") != 5:
        fail(errors, "v0.105.0 implementation status is incomplete")
    if "0.105.0" not in (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"):
        fail(errors, "CHANGELOG.md is missing the v0.105.0 entry")

    archive = ROOT / f"sql/archive/pg_trickle--{VERSION}.sql"
    migration = ROOT / "sql/pg_trickle--0.104.0--0.105.0.sql"
    if not archive.is_file():
        fail(errors, "full SQL archive is missing")
    if not migration.is_file():
        fail(errors, "upgrade migration is missing")
    elif "0.105.0" not in migration.read_text(encoding="utf-8"):
        fail(errors, "upgrade migration does not record 0.105.0")

    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    for marker in (
        "python3 scripts/v0_105_0_release_gate.py",
        "qualification:",
        "bash ./scripts/run_light_e2e_tests.sh",
        "--test e2e_v104_conformance_tests",
        "--qualification tests/release/v0.105.0-qualification.json",
        "--postgresql-version",
        "--suite graph-v1=passed",
        "--suite delta-v1=passed",
        "--log",
    ):
        if marker not in workflow:
            fail(errors, f"release workflow is missing {marker}")

    release_evidence = (ROOT / "scripts/release_evidence.py").read_text(encoding="utf-8")
    for marker in ("--postgresql-version", "--suite-version", "--workload", "--log", "required_suite_ids"):
        if marker not in release_evidence:
            fail(errors, f"release evidence writer is missing {marker}")

    subprocess.run(
        [sys.executable, str(ROOT / "scripts/generate_capability_manifest.py"), "--check"],
        cwd=ROOT,
        check=True,
    )

    if errors:
        print("v0_105_0_release_gate: FAILED")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("v0_105_0_release_gate: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
