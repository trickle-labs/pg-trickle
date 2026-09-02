#!/usr/bin/env python3
"""Offline release gate for the v0.92 backup, upgrade, and recovery contract."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VERSION = "0.92.0"

REQUIRED_FILES = (
    "roadmap/v0.92.0.md",
    "roadmap/v0.92.0.md-full.md",
    "plans/PLAN_0_92_0.md",
    "sql/pg_trickle--0.91.0--0.92.0.sql",
    "sql/archive/pg_trickle--0.92.0.sql",
    "docs/upgrade-support-manifest.json",
    "docs/UPGRADING.md",
)

REQUIRED_SOURCE_MARKERS = {
    "src/api/recovery.rs": (
        "RecoveryClass",
        "CDC_TRIGGER_MISSING",
        "CDC_SLOT_MISSING",
        "CDC_WAL_UNAVAILABLE",
        "CDC_BUFFER_MISSING",
        "CDC_CLONE_DETECTED",
        "validate_recovery",
        "preflight_upgrade",
        "quiesce",
        "resume_all",
        "recover_capture_instance",
        "FRONTIER_UNPROVEN",
    ),
    "src/scheduler/scheduler_loop.rs": (
        "capture_gate_allows_work",
        "capture is quarantined or quiesced",
    ),
}

REQUIRED_TESTS = (
    "test_recovery_capture_instance_catalog_and_safe_report",
    "test_recovery_quiesce_and_resume_boundary",
    "test_recovery_clone_isolation_requires_explicit_adoption",
    "test_recovery_missing_trigger_is_reinitialization_required",
    "test_recovery_missing_slot_is_reinitialization_required",
    "test_recovery_frontier_ahead_of_wal_fails_closed",
    "test_upgrade_preflight_reports_stable_statuses",
    "test_upgrade_quiesce_preserves_pending_deltas",
    "test_upgrade_major_version_active_stream_table_with_pending_deltas",
)


def main() -> int:
    errors: list[str] = []

    for relative in REQUIRED_FILES:
        if not (ROOT / relative).is_file():
            errors.append(f"missing v0.92 artifact: {relative}")

    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    if f'version = "{VERSION}"' not in cargo:
        errors.append(f"Cargo.toml does not declare {VERSION}")

    migration = ROOT / "sql/pg_trickle--0.91.0--0.92.0.sql"
    if migration.is_file():
        migration_text = migration.read_text(encoding="utf-8")
        for marker in (
            "pgt_capture_instance",
            "capture_instance_status",
            "validate_recovery",
            "quiesce",
            "resume_all",
            "recover_capture_instance",
            "preflight_upgrade",
            "pgt_schema_version",
        ):
            if marker not in migration_text:
                errors.append(f"migration missing v0.92 marker: {marker}")

    for relative, markers in REQUIRED_SOURCE_MARKERS.items():
        path = ROOT / relative
        if not path.is_file():
            continue
        source = path.read_text(encoding="utf-8")
        for marker in markers:
            if marker not in source:
                errors.append(f"{relative} missing exact v0.92 marker: {marker}")

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
            if manifest.get("source_version_lower_bound") != "0.40.0":
                errors.append("support manifest lower bound must be 0.40.0")
            if manifest.get("source_version_upper_bound") != "0.98.x":
                errors.append("support manifest upper bound must be 0.98.x")
            if not versions or versions[0] != "0.40.0" or versions[-1] != VERSION:
                errors.append("support manifest must cover released sources through 0.92.0")
            if len(versions) != len(set(versions)):
                errors.append("support manifest contains duplicate source versions")
            if any(not re.fullmatch(r"0\.(?:4[0-9]|[5-8][0-9]|9[0-8])(?:\.\d+)?", v) for v in versions):
                errors.append("support manifest contains a source version outside the v0.40-v0.98 bound")

    test_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for directory in (ROOT / "tests", ROOT / "src")
        if directory.is_dir()
        for path in directory.rglob("*.rs")
        if path.is_file()
    )
    for test_name in REQUIRED_TESTS:
        if not re.search(rf"\bfn\s+{re.escape(test_name)}\b", test_sources):
            errors.append(f"missing exact v0.92 recovery test: {test_name}")

    if errors:
        print("v0_92_release_gate: FAILED")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("v0_92_release_gate: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
