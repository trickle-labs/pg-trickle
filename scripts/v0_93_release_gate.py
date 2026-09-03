#!/usr/bin/env python3
"""Offline release gate for the v0.93 capability and contract surface."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VERSION = "0.93.0"

REQUIRED_FILES = (
    "roadmap/v0.93.0.md",
    "plans/PLAN_0_93_0.md",
    "sql/pg_trickle--0.92.0--0.93.0.sql",
    "sql/archive/pg_trickle--0.93.0.sql",
    "docs/UPGRADING.md",
    "docs/upgrade-support-manifest.json",
)

REQUIRED_SOURCE_MARKERS = {
    "src/integration_contract.rs": (
        "CONTRACT_ENCODING_VERSION",
        "CanonicalValue",
        "contract_digest",
    ),
    "src/api/integration.rs": (
        "integration_capabilities",
        "stream_table_contract",
        "graph_contract",
        "set_orchestration_mode",
    ),
    "src/catalog.rs": (
        "orchestration_mode",
        "contract_generation",
        "update_orchestration_mode",
    ),
    "src/api/create.rs": ("orchestration_mode",),
    "src/api/alter.rs": ("orchestration_mode",),
    "src/scheduler/mod.rs": ("orchestration_mode",),
}

REQUIRED_TESTS = (
    "test_integration_capabilities_reports_independent_capabilities",
    "test_stream_table_contract_is_deterministic",
    "test_graph_contract_canonicalizes_closure_and_order",
    "test_external_orchestration_excludes_scheduler",
    "test_external_graph_authorization_fails_closed",
)


def main() -> int:
    errors: list[str] = []

    for relative in REQUIRED_FILES:
        if not (ROOT / relative).is_file():
            errors.append(f"missing v0.93 artifact: {relative}")

    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    if f'version = "{VERSION}"' not in cargo:
        errors.append(f"Cargo.toml does not declare {VERSION}")

    migration = ROOT / "sql/pg_trickle--0.92.0--0.93.0.sql"
    if migration.is_file():
        migration_text = migration.read_text(encoding="utf-8")
        for marker in (
            "orchestration_mode",
            "contract_generation",
            "MANAGED",
            "EXTERNAL",
            "pgt_schema_version",
        ):
            if marker not in migration_text:
                errors.append(f"migration missing v0.93 marker: {marker}")

    for relative, markers in REQUIRED_SOURCE_MARKERS.items():
        path = ROOT / relative
        if not path.is_file():
            for marker in markers:
                errors.append(f"{relative} missing exact v0.93 marker: {marker}")
            continue
        source = path.read_text(encoding="utf-8")
        for marker in markers:
            if marker not in source:
                errors.append(f"{relative} missing exact v0.93 marker: {marker}")

    manifest_path = ROOT / "docs/upgrade-support-manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            errors.append(f"support manifest is invalid JSON: {error}")
        else:
            versions = manifest.get("supported_source_versions", [])
            if manifest.get("postgresql_majors") != [18]:
                errors.append("support manifest must be limited to PostgreSQL 18")
            if not versions or versions[-1] != VERSION:
                errors.append("support manifest must include 0.93.0")
            if len(versions) != len(set(versions)):
                errors.append("support manifest contains duplicate source versions")
            if any(not re.fullmatch(r"0\.\d+(?:\.\d+|\.x)?", v) for v in versions):
                errors.append("support manifest contains an invalid source version")

    test_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for directory in (ROOT / "tests", ROOT / "src")
        if directory.is_dir()
        for path in directory.rglob("*.rs")
        if path.is_file()
    )
    for test_name in REQUIRED_TESTS:
        if not re.search(rf"\bfn\s+{re.escape(test_name)}\b", test_sources):
            errors.append(f"missing exact v0.93 contract test: {test_name}")

    if errors:
        print("v0_93_release_gate: FAILED")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("v0_93_release_gate: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
