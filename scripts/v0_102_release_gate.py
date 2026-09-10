#!/usr/bin/env python3
"""Validate the v0.102 output-sensitive evidence release contract."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.102.0"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"v0.102 release gate failed: {message}")


def main() -> None:
    with (ROOT / "Cargo.toml").open("rb") as handle:
        require(
            tomllib.load(handle)["package"]["version"] == VERSION,
            "Cargo.toml version drift",
        )

    meta = json.loads((ROOT / "META.json").read_text(encoding="utf-8"))
    require(meta["version"] == VERSION, "META.json top-level version drift")
    require(
        meta["provides"]["pg_trickle"]["version"] == VERSION,
        "META.json provides version drift",
    )
    require(
        (ROOT / f"sql/archive/pg_trickle--{VERSION}.sql").is_file(),
        "full SQL archive missing",
    )
    require(
        (ROOT / "sql/pg_trickle--0.101.0--0.102.0.sql").is_file(),
        "upgrade migration missing",
    )

    source = (ROOT / "src/refresh/mod.rs").read_text(encoding="utf-8")
    catalog = (ROOT / "src/catalog.rs").read_text(encoding="utf-8")
    merge = (ROOT / "src/refresh/merge/mod.rs").read_text(encoding="utf-8")
    require(
        "build_differential_cost_evidence" in source
        and "output_amplification_ratio" in source
        and "set_last_cost_evidence" in merge,
        "output-sensitive evidence is not wired into differential refreshes",
    )
    require(
        "set_cost_evidence" in catalog,
        "refresh history does not persist cost evidence",
    )

    manifest = json.loads(
        (ROOT / "docs/capability-manifest.json").read_text(encoding="utf-8")
    )
    require(manifest["release_version"] == VERSION, "capability manifest version drift")
    require(
        any(
            item.get("test") == "test_differential_cost_evidence_reports_output_amplification"
            for item in manifest["examples"]
        ),
        "cost-evidence release example is missing",
    )

    roadmap = (ROOT / "roadmap/v0.102.0.md").read_text(encoding="utf-8")
    require("> **Status:** Released" in roadmap, "roadmap release status is stale")
    roadmap_index = (ROOT / "ROADMAP.md").read_text(encoding="utf-8")
    require(
        "[v0.102.0]" in roadmap_index and "✅ Released" in roadmap_index,
        "roadmap index is stale",
    )
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    require("0.102.0" in changelog, "CHANGELOG.md is missing the release entry")

    subprocess.run(
        [sys.executable, str(ROOT / "scripts/generate_capability_manifest.py"), "--check"],
        cwd=ROOT,
        check=True,
    )
    print("v0.102 release gate passed")


if __name__ == "__main__":
    main()
