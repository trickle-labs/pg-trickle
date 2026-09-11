#!/usr/bin/env python3
"""Validate the v0.104 stable integration-contract release surfaces."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.104.0"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"v0.104 release gate failed: {message}")


def main() -> None:
    cargo = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    require(cargo["package"]["version"] == VERSION, "Cargo.toml version drift")

    lock = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
    require(re.search(r'name = "pg_trickle"\s+version = "0\.104\.0"', lock) is not None, "Cargo.lock version drift")

    meta = json.loads((ROOT / "META.json").read_text(encoding="utf-8"))
    require(meta["version"] == VERSION, "META.json top-level version drift")
    require(meta["provides"]["pg_trickle"]["version"] == VERSION, "META.json provides version drift")

    archive = ROOT / f"sql/archive/pg_trickle--{VERSION}.sql"
    migration = ROOT / "sql/pg_trickle--0.103.0--0.104.0.sql"
    require(archive.is_file(), "full SQL archive missing")
    require(migration.is_file(), "upgrade migration missing")
    archive_text = archive.read_text(encoding="utf-8")
    migration_text = migration.read_text(encoding="utf-8")
    require("EXTERNAL_GRAPH" in archive_text, "archive omits external graph history provenance")
    require("EXTERNAL_GRAPH" in migration_text, "migration omits external graph history provenance")

    manifest = json.loads((ROOT / "docs/capability-manifest.json").read_text(encoding="utf-8"))
    require(manifest["release_version"] == VERSION, "capability manifest version drift")
    capabilities = {item["id"]: item for item in manifest["capabilities"]}
    for capability in ("graph-v1", "delta-v1"):
        item = capabilities.get(capability)
        require(item is not None, f"manifest omits {capability}")
        require(item["status"] == "stable" and item["enabled"], f"{capability} is not stable and enabled")
        require(item["strategy"] == "public_sql", f"{capability} is not admitted through public SQL")

    conformance = json.loads((ROOT / "tests/release/v0.104-conformance.json").read_text(encoding="utf-8"))
    require(conformance["release_version"] == VERSION, "conformance contract version drift")
    suite_ids = {suite["id"] for suite in conformance["required_suites"]}
    require({"graph-v1", "delta-v1"} <= suite_ids, "required conformance suites are incomplete")
    require("reference clients use public SQL only" in conformance["scope_boundaries"], "public API boundary is missing")

    tests = (ROOT / "tests/e2e_v104_conformance_tests.rs").read_text(encoding="utf-8")
    for test in (
        "test_v104_capabilities_are_stable_by_default",
        "test_v104_graph_reference_coordinator_commits_publication",
        "test_v104_delta_reference_consumer_reads_and_acknowledges",
    ):
        require(re.search(rf"\basync fn {test}\b", tests) is not None, f"conformance test missing: {test}")

    roadmap = (ROOT / "roadmap/v0.104.0.md").read_text(encoding="utf-8")
    require("> **Status:** Released" in roadmap, "roadmap release status is stale")
    roadmap_index = (ROOT / "ROADMAP.md").read_text(encoding="utf-8")
    require("[v0.104.0]" in roadmap_index and "✅ Released" in roadmap_index, "roadmap index is stale")
    require("0.104.0" in (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), "CHANGELOG.md is missing the release entry")
    require("v0.104" in (ROOT / "docs/SQL_REFERENCE.md").read_text(encoding="utf-8"), "SQL reference is missing v0.104 contract notes")

    subprocess.run(
        [sys.executable, str(ROOT / "scripts/generate_capability_manifest.py"), "--check"],
        cwd=ROOT,
        check=True,
    )
    print("v0.104 release gate passed")


if __name__ == "__main__":
    main()
