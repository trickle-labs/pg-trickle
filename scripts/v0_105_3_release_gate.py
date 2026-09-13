#!/usr/bin/env python3
"""Validate v0.105.3 regressions, candidate evidence, and publication gates."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.105.3"
QUALIFICATION = ROOT / "tests/release/v0.105.3-qualification.json"
EXPECTED_SUITES = {
    "graph-v1",
    "delta-v1",
    "dvm-oracle",
    "pre-fix-reproduction",
    "affected-state-repair",
    "recovery",
    "wal-admission",
    "upgrade-chain",
    "version-sync",
    "package-smoke",
    "monitoring-contract",
    "release-artifact-smoke",
    "publication-gate",
    "operations",
    "performance",
}
EXPECTED_ARTIFACTS = {"linux-amd64", "linux-arm64", "macos-arm64", "windows-amd64"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"v0.105.3 release gate failed: {message}")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"v0.105.3 release gate failed: {path.relative_to(ROOT)}: {error}") from error
    require(isinstance(value, dict), f"{path.relative_to(ROOT)} must contain an object")
    return value


def job_block(workflow: str, name: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)", workflow
    )
    require(match is not None, f"workflow job {name!r} is missing")
    return match.group(1)


def needs(block: str, job: str) -> set[str]:
    match = re.search(r"(?m)^    needs:\s*(.+)$", block)
    require(match is not None, f"job {job!r} has no explicit needs dependency")
    return set(re.findall(r"[A-Za-z0-9_-]+", match.group(1)))


def check_publication_gate() -> None:
    release = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    for job in ("publish-release", "publish-docker-arch"):
        block = job_block(release, job)
        require("qualification" in needs(block, job), f"{job} can publish without qualification")
        require("always()" not in block, f"{job} must keep GitHub's default success dependency gate")
    docker_manifest = job_block(release, "publish-docker")
    require("publish-docker-arch" in needs(docker_manifest, "publish-docker"), "CNPG manifest is not gated by architecture images")

    result_gate = job_block(release, "publication-result")
    required_jobs = {
        "build-release",
        "test-release",
        "qualification",
        "publish-release",
        "publish-docker-arch",
        "publish-docker",
    }
    require(required_jobs <= needs(result_gate, "publication-result"), "final release result does not wait for every publishing job")
    require("if: always()" in result_gate, "final release result must run when an upstream job is skipped or fails")
    require("needs.qualification.result" in result_gate, "final release result does not check qualification status")

    for relative, job in ((".github/workflows/ghcr.yml", "preflight"), (".github/workflows/pgxn.yml", "pgxn")):
        workflow = (ROOT / relative).read_text(encoding="utf-8")
        require('workflows: ["Release"]' in workflow and "types: [completed]" in workflow, f"{relative} must wait for Release completion")
        trigger = re.search(r"(?ms)^on:\n(.*?)(?=^[A-Za-z][A-Za-z0-9_-]*:\n|\Z)", workflow)
        require(trigger is not None and "workflow_run:" in trigger.group(1), f"{relative} has no workflow_run trigger")
        require("push:" not in trigger.group(1) and "workflow_dispatch:" not in trigger.group(1), f"{relative} has an ungated publish trigger")
        block = job_block(workflow, job)
        require("workflow_run.conclusion == 'success'" in block, f"{relative} does not require a successful Release workflow")
        require("startsWith(github.event.workflow_run.head_branch, 'v')" in block, f"{relative} does not restrict promotion to version tags")
        require("github.event.workflow_run.head_sha" in workflow, f"{relative} does not use the qualified candidate commit")

    # Negative control: a failed, skipped, or cancelled qualification must make
    # every native `needs` publisher non-runnable and the final result fail.
    for qualification_status in ("failure", "skipped", "cancelled"):
        statuses = {name: "success" for name in required_jobs}
        statuses["qualification"] = qualification_status
        release_publish = all(statuses[name] == "success" for name in needs(job_block(release, "publish-release"), "publish-release"))
        docker_publish = all(statuses[name] == "success" for name in needs(job_block(release, "publish-docker-arch"), "publish-docker-arch"))
        statuses["publish-release"] = "success" if release_publish else "skipped"
        statuses["publish-docker-arch"] = "success" if docker_publish else "skipped"
        statuses["publish-docker"] = "success" if statuses["publish-docker-arch"] == "success" else "skipped"
        require(not release_publish and not docker_publish, f"{qualification_status} qualification allowed a release publisher")
        require(not all(statuses[name] == "success" for name in required_jobs), f"{qualification_status} qualification did not fail the final result")
        release_conclusion = "success" if all(statuses[name] == "success" for name in required_jobs) else "failure"
        external_publish = release_conclusion == "success" and "v0.105.3".startswith("v")
        require(not external_publish, f"{qualification_status} qualification allowed GHCR or PGXN promotion")


def main() -> None:
    require(tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))["package"]["version"] == VERSION, "Cargo.toml version drift")
    contract = read_json(QUALIFICATION)
    require(contract.get("release_version") == VERSION, "qualification version drift")
    require(contract.get("source_versions") == ["0.105.2"], "source version boundary drift")
    suites = contract.get("required_suites")
    require(isinstance(suites, list), "required_suites must be a list")
    suite_ids = {suite.get("id") for suite in suites if isinstance(suite, dict)}
    require(suite_ids == EXPECTED_SUITES and len(suite_ids) == len(suites), "qualification suite set is incomplete or duplicated")
    require(all(suite.get("required") is True and suite.get("enabled") is True and suite.get("expected_status") == "passed" for suite in suites), "every qualification suite must be enabled and required")

    artifacts = contract.get("artifacts")
    require(isinstance(artifacts, list), "artifact contract must be a list")
    require({item.get("id") for item in artifacts if isinstance(item, dict)} == EXPECTED_ARTIFACTS, "artifact set is incomplete")
    require(all(item.get("version") == VERSION and item.get("postgresql_major") == 18 and item.get("build_status") == "passed" for item in artifacts), "artifact build metadata drift")
    runtime = {item["id"] for item in artifacts if item.get("runtime_status") == "passed"}
    require(runtime == {"linux-amd64"}, "runtime qualification scope must identify the one exercised package")
    controls = {item["id"] for item in artifacts if item.get("control_suites") == ["pre-fix-reproduction"]}
    require(controls == {"linux-amd64"}, "pre-fix reproduction must be tied to the Linux amd64 qualification runner")

    tests = (ROOT / "tests/e2e_diff_full_equivalence_tests.rs").read_text(encoding="utf-8")
    for name in (
        "test_diff_full_equivalence_exists_and_not_exists_simultaneous_changes",
        "test_diff_aggregate_filter_above_projected_subquery_with_inner_where",
        "test_diff_reinitialize_repairs_one_table_after_committed_source_change",
    ):
        require(re.search(rf"\basync\s+fn\s+{re.escape(name)}\b", tests) is not None, f"missing regression {name}")

    migration = ROOT / "sql/pg_trickle--0.105.2--0.105.3.sql"
    archive = ROOT / "sql/archive/pg_trickle--0.105.3.sql"
    require(migration.is_file() and VERSION in migration.read_text(encoding="utf-8"), "upgrade migration is missing or unversioned")
    require(archive.is_file(), "full SQL archive is missing")
    require("[0.105.3]" in (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), "changelog entry is missing")
    require("> **Status:** Released" in (ROOT / "roadmap/v0.105.3.md").read_text(encoding="utf-8"), "roadmap release status is stale")

    check_publication_gate()
    release_workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    for marker in (
        "Reproduce reported regressions against v0.105.2",
        "test_diff_full_equivalence_exists_and_not_exists_simultaneous_changes",
        "test_diff_aggregate_filter_above_projected_subquery_with_inner_where",
        "--suite pre-fix-reproduction=passed",
    ):
        require(marker in release_workflow, f"release workflow is missing {marker}")
    subprocess.run([sys.executable, str(ROOT / "scripts/generate_capability_manifest.py"), "--check"], cwd=ROOT, check=True)
    subprocess.run([str(ROOT / "scripts/check_version_sync.sh")], cwd=ROOT, check=True)
    print("v0.105.3 DVM and publication qualification gate passed, including failed-gate negative control")


if __name__ == "__main__":
    main()
