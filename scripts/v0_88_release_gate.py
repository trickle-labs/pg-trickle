#!/usr/bin/env python3
"""Offline release gate for the v0.88.0 engine contract."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def read_json(path: str, errors: list[str]) -> dict:
    try:
        value = json.loads(read(path))
    except (OSError, json.JSONDecodeError) as error:
        errors.append(f"invalid v0.88 artifact {path}: {error}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"v0.88 artifact {path} must contain a JSON object")
        return {}
    return value


def main() -> int:
    errors: list[str] = []
    cargo = read("Cargo.toml")
    version = re.search(r'^version\s*=\s*"([^"]+)"', cargo, re.MULTILINE)
    if not version or version.group(1) != "0.88.0":
        errors.append("Cargo.toml version must be 0.88.0")

    for path in (
        "sql/archive/pg_trickle--0.88.0.sql",
        "sql/pg_trickle--0.87.17--0.88.0.sql",
        "plans/adrs/ADR-009.md",
        "plans/adrs/ADR-010.md",
        "src/dvm/operators/vectorized_agg.rs",
        "src/refresh/vectorized_agg.rs",
        "src/dvm/planner.rs",
    ):
        if not (ROOT / path).exists():
            errors.append(f"missing v0.88 artifact: {path}")

    if any(crate in cargo for crate in ("arrow-array", "arrow-compute")):
        errors.append("ADR-009 forbids Arrow dependencies for v0.88.0")

    source = "\n".join(
        read(path)
        for path in (
            "src/dvm/diff.rs",
            "src/dvm/operators/vectorized_agg.rs",
            "src/dvm/planner.rs",
            "src/refresh/merge/mod.rs",
            "src/api/diagnostics.rs",
        )
        if (ROOT / path).exists()
    )
    for symbol in (
        "CdcContext",
        "CacheContext",
        "OptimizationContext",
        "VectorizedAggregateOperator",
        "explain_delta_plan",
        "EXCEPT ALL",
    ):
        if symbol not in source:
            errors.append(f"missing engine contract: {symbol}")

    config = read("src/config/dvm.rs")
    if 'c"pg_trickle.merge_batch_size"' in config:
        errors.append("deprecated pg_trickle.merge_batch_size is still registered")

    contract = json.loads(read("benchmarks/vector-aggregate-v0.88/contract.json"))
    expected = {
        "source_rows": 1_000_000,
        "groups": 10_000,
        "changed_rows": 100_000,
        "page_rows": 1_024,
        "warmups": 1,
        "measured_refreshes": 5,
        "minimum_throughput_ratio": 5.0,
    }
    for key, value in expected.items():
        if contract.get(key) != value:
            errors.append(f"benchmark contract {key} must equal {value}")

    baseline = read_json(
        "benchmarks/vector-aggregate-v0.88/baseline-v0.87.17.json", errors
    )
    result = read_json("benchmarks/vector-aggregate-v0.88/result-v0.88.0.json", errors)
    comparison = read_json("benchmarks/vector-aggregate-v0.88/comparison.json", errors)
    if baseline.get("extension_version") != "0.87.17":
        errors.append("vector benchmark baseline must use extension 0.87.17")
    if result.get("extension_version") != "0.88.0":
        errors.append("vector benchmark result must use extension 0.88.0")
    for label, artifact in (("baseline", baseline), ("result", result)):
        if artifact.get("measured_refreshes") != contract["measured_refreshes"]:
            errors.append(f"vector benchmark {label} must contain five measured refreshes")
        if artifact.get("exact_multiset_validated") is not True:
            errors.append(f"vector benchmark {label} failed exact multiset validation")
    if result.get("merge_strategy") != "vector_agg":
        errors.append("v0.88 vector benchmark did not select vector_agg")
    ratio = result.get("throughput_ratio_vs_baseline")
    if not isinstance(ratio, (int, float)) or ratio < contract["minimum_throughput_ratio"]:
        errors.append("v0.88 vector benchmark throughput ratio is below 5x")
    if comparison.get("gate_passed") is not True or comparison.get("throughput_ratio") != ratio:
        errors.append("vector benchmark comparison does not match the passing result")

    docs = read("docs/SQL_REFERENCE.md")
    if "pgtrickle.explain_delta_plan" not in docs:
        errors.append("SQL reference omits explain_delta_plan")

    if errors:
        print("v0_88_release_gate: FAILED")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("v0_88_release_gate: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
