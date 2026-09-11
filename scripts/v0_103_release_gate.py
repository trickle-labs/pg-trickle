#!/usr/bin/env python3
"""Validate the v0.103 durable WAL receipt release contract."""

from __future__ import annotations

import json
import math
import re
import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.103.0"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"v0.103 release gate failed: {message}")


def main() -> None:
    with (ROOT / "Cargo.toml").open("rb") as handle:
        require(tomllib.load(handle)["package"]["version"] == VERSION, "Cargo.toml version drift")

    lock = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
    require(
        re.search(r'name = "pg_trickle"\s+version = "0\.103\.0"', lock) is not None,
        "Cargo.lock package version drift",
    )

    meta = json.loads((ROOT / "META.json").read_text(encoding="utf-8"))
    require(meta["version"] == VERSION, "META.json top-level version drift")
    require(meta["provides"]["pg_trickle"]["version"] == VERSION, "META.json provides version drift")

    archive = ROOT / f"sql/archive/pg_trickle--{VERSION}.sql"
    migration = ROOT / "sql/pg_trickle--0.102.0--0.103.0.sql"
    require(archive.is_file(), "full SQL archive missing")
    require(migration.is_file(), "upgrade migration missing")
    require("pgt_wal_receipts" in archive.read_text(encoding="utf-8"), "archive omits WAL receipts")
    require("pgt_wal_receipts" in migration.read_text(encoding="utf-8"), "migration omits WAL receipts")

    decoder = (ROOT / "src/wal_decoder.rs").read_text(encoding="utf-8")
    require("pg_logical_slot_peek_changes" in decoder, "WAL polling still acknowledges before receipt persistence")
    require("pub fn replay_pending_wal_receipts" in decoder, "receipt replay is not wired")
    require("pub fn acknowledge_pending_wal_receipts" in decoder, "receipt acknowledgement is not wired")
    require("receipt_high_water_lsn" in decoder and "acknowledged_high_water_lsn" in decoder, "receipt watermarks are missing")

    budget_path = ROOT / "tests/release/v0.103-workload-budgets.json"
    budget = json.loads(budget_path.read_text(encoding="utf-8"))
    require(budget.get("release_version") == VERSION, "workload budget version drift")
    workloads = budget.get("workloads")
    require(isinstance(workloads, list) and workloads, "workload budget is empty")
    required = {
        "id",
        "hardware_class",
        "sample_count",
        "max_foreground_write_regression",
        "max_decoding_amplification_ratio",
    }
    ids = set()
    for workload in workloads:
        require(required <= workload.keys(), f"workload budget fields missing: {workload}")
        require(workload["id"] not in ids, f"duplicate workload id: {workload['id']}")
        ids.add(workload["id"])
        require(workload["sample_count"] > 0, f"workload has no samples: {workload['id']}")
        for key in required - {"id", "hardware_class", "sample_count"}:
            value = workload[key]
            require(isinstance(value, (int, float)) and math.isfinite(value) and value >= 0, f"invalid {key} for {workload['id']}")

    roadmap = (ROOT / "roadmap/v0.103.0.md").read_text(encoding="utf-8")
    require("> **Status:** Released" in roadmap, "roadmap release status is stale")
    roadmap_index = (ROOT / "ROADMAP.md").read_text(encoding="utf-8")
    require("[v0.103.0]" in roadmap_index and "✅ Released" in roadmap_index, "roadmap index is stale")
    require("0.103.0" in (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), "CHANGELOG.md is missing the release entry")

    subprocess.run(
        [sys.executable, str(ROOT / "scripts/generate_capability_manifest.py"), "--check"],
        cwd=ROOT,
        check=True,
    )
    print("v0.103 release gate passed")


if __name__ == "__main__":
    main()
