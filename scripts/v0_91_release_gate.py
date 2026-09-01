#!/usr/bin/env python3
"""Offline release gate for the v0.91 schema-evolution contract."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

REQUIRED_ARTIFACTS = (
    "roadmap/v0.91.0.md",
    "roadmap/v0.91.0.md-full.md",
    "sql/pg_trickle--0.90.0--0.91.0.sql",
    "sql/archive/pg_trickle--0.91.0.sql",
)

REQUIRED_SOURCE_MARKERS = {
    "src/api/alter.rs": (
        "AlterClassification",
        "Compatible",
        "Rebuildable",
        "Rejected",
        "AlterStateOracle",
        "explain_alter",
        "atomic_swap_shadow_table",
        "resume_or_rollback_shadow_build",
    ),
    "src/hooks.rs": (
        "pgt_dependencies",
        "source_oid",
        "mark_for_reinitialize",
        "SchemaChangeKind",
        "AlterReasonCode",
        "SOURCE_RECREATED",
    ),
    "src/monitor/health.rs": (
        "health_check",
        "refresh_reason",
        "SOURCE_",
        "reinitialize_stream_table",
    ),
}

REQUIRED_TESTS = (
    "test_explain_alter_classifies_compatible_without_mutation",
    "test_explain_alter_classifies_rebuildable_without_mutation",
    "test_explain_alter_rejects_without_mutation",
    "test_alter_query_proves_materialized_result_frontier_row_identity_auxiliary_state",
    "test_shadow_rebuild_atomic_swap_preserves_old_result_until_cutover",
    "test_shadow_rebuild_interruption_resumes_or_rolls_back",
    "test_schema_evolution_transactional_ddl_rollback",
    "test_schema_evolution_additive_column_continues_or_rebuilds",
    "test_schema_evolution_destructive_column_suspends_with_reason_code",
    "test_schema_evolution_rename_chain_preserves_or_suspends_explicitly",
    "test_schema_evolution_schema_move_and_owner_change",
    "test_schema_evolution_recreated_object_oid_is_not_reused",
    "test_schema_evolution_dependency_oid_change_invalidates_proof",
    "test_schema_evolution_refresh_concurrent_with_ddl",
    "test_schema_evolution_extension_upgrade_overlap",
)

FORBIDDEN_AMBIGUOUS_CLASSIFICATION = re.compile(
    r"SchemaChange::Incompatible|\bIncompatible\s*\{", re.MULTILINE
)


def main() -> int:
    errors: list[str] = []

    for relative in REQUIRED_ARTIFACTS:
        if not (ROOT / relative).is_file():
            errors.append(f"missing v0.91 contract artifact: {relative}")

    for relative, markers in REQUIRED_SOURCE_MARKERS.items():
        path = ROOT / relative
        if not path.is_file():
            errors.append(f"missing v0.91 implementation file: {relative}")
            continue
        source = path.read_text(encoding="utf-8")
        for marker in markers:
            if marker not in source:
                errors.append(f"{relative} missing exact v0.91 marker: {marker}")
        if relative == "src/api/alter.rs" and FORBIDDEN_AMBIGUOUS_CLASSIFICATION.search(
            source
        ):
            errors.append(
                "src/api/alter.rs retains the pre-v0.91 ambiguous Incompatible classification"
            )

    test_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for directory in (ROOT / "tests", ROOT / "src")
        if directory.is_dir()
        for path in directory.rglob("*.rs")
        if path.is_file()
    )
    for test_name in REQUIRED_TESTS:
        if not re.search(rf"\bfn\s+{re.escape(test_name)}\b", test_sources):
            errors.append(f"missing exact v0.91 deterministic test: {test_name}")

    if errors:
        print("v0_91_release_gate: FAILED")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("v0_91_release_gate: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
