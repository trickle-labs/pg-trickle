#!/usr/bin/env python3
"""Offline release gate for the v0.90 freshness-controller contract."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

REQUIRED_ARTIFACTS = (
    "roadmap/v0.90.0.md",
    "plans/PLAN_0_90_0.md",
    "sql/pg_trickle--0.89.0--0.90.0.sql",
    "sql/archive/pg_trickle--0.90.0.sql",
    "src/scheduler/controller.rs",
)

FRESHNESS_MARKERS = (
    "source_xid",
    "source_commit_at",
    "visibility_xid",
    "visible_at",
    "commit_to_visible_ms",
    "duration_ms",
    "plan_identity",
    "pgtrickle.pgt_freshness_controller_state",
    "p95_freshness_ms",
    "sla_status",
)

API_MARKERS = (
    "pgtrickle.freshness",
    "stream_table",
    "target",
    "p50",
    "p95",
    "p99",
    "status",
)

CONTROLLER_MARKERS = (
    "FreshnessDistribution",
    "ControllerInputs",
    "ControllerDecision",
    "ControllerHysteresis",
    "target_ms",
    "deadline_slack_ms",
    "reason_code",
)

FORBIDDEN_DURATION_FALLBACKS = (
    re.compile(r"commit_to_visible_ms\s*=\s*[^\n;]*\b(?:end_time|start_time|duration_ms)\b", re.IGNORECASE),
)


def active_implementation() -> list[str]:
    paths = (
        path
        for directory in (ROOT / "src", ROOT / "sql")
        if directory.is_dir()
        for path in directory.rglob("*")
        if path.is_file() and path.suffix in {".rs", ".sql"}
    )
    return [path.read_text(encoding="utf-8") for path in paths]


def main() -> int:
    errors: list[str] = []
    for relative in REQUIRED_ARTIFACTS:
        if not (ROOT / relative).is_file():
            errors.append(f"missing v0.90 contract artifact: {relative}")

    implementation_sources = active_implementation()
    implementation = "\n".join(implementation_sources)
    for marker in FRESHNESS_MARKERS:
        if marker not in implementation:
            errors.append(f"missing exact freshness marker: {marker}")
    if not any(
        all(marker in source for marker in API_MARKERS)
        for source in implementation_sources
    ):
        errors.append("freshness API must expose the exact six-column contract")

    controller = ROOT / "src/scheduler/controller.rs"
    if controller.is_file():
        controller_source = controller.read_text(encoding="utf-8")
        for marker in CONTROLLER_MARKERS:
            if marker not in controller_source:
                errors.append(f"missing controller marker: {marker}")

    for pattern in FORBIDDEN_DURATION_FALLBACKS:
        if pattern.search(implementation):
            errors.append("active implementation contains a duration fallback")
            break

    if errors:
        print("v0_90_release_gate: FAILED")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("v0_90_release_gate: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
