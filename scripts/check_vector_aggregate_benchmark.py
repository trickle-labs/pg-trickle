#!/usr/bin/env python3
"""Check the saved vector-aggregate benchmark contract and results."""

from __future__ import annotations

import json
import math
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "benchmarks" / "vector-aggregate-v0.88"


def read_json(name: str) -> dict:
    with (DATA / name).open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return value


def main() -> int:
    try:
        contract = read_json("contract.json")
        baseline = read_json("baseline-v0.87.17.json")
        result = read_json("result-v0.88.0.json")
        comparison = read_json("comparison.json")
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"Invalid vector-aggregate benchmark evidence: {error}", file=sys.stderr)
        return 1

    errors = []
    expected = {
        "baseline_version": "0.87.17",
        "source_rows": 1_000_000,
        "groups": 10_000,
        "changed_rows": 100_000,
        "page_rows": 1_024,
        "warmups": 1,
        "measured_refreshes": 5,
        "minimum_throughput_ratio": 5.0,
        "maximum_eligible_workload_regression": 0.1,
        "validation": "exact_multiset_after_each_refresh",
    }
    for key, value in expected.items():
        if contract.get(key) != value:
            errors.append(f"contract {key} must equal {value}")
    if errors:
        print("vector aggregate benchmark evidence failed")
        print("\n".join(f"- {error}" for error in errors))
        return 1

    if baseline.get("extension_version") != contract["baseline_version"]:
        errors.append("baseline extension version does not match the contract")
    if result.get("extension_version") != "0.88.0":
        errors.append("candidate result must use extension 0.88.0")
    for label, artifact in (("baseline", baseline), ("result", result)):
        if artifact.get("measured_refreshes") != contract["measured_refreshes"]:
            errors.append(f"{label} must contain five measured refreshes")
        if artifact.get("exact_multiset_validated") is not True:
            errors.append(f"{label} failed exact multiset validation")

    if result.get("merge_strategy") != "vector_agg":
        errors.append("candidate result did not select vector_agg")
    ratio = result.get("throughput_ratio_vs_baseline")
    baseline_ms = baseline.get("median_ms")
    result_ms = result.get("median_ms")
    valid_measurements = all(
        type(value) in (int, float) and math.isfinite(value) and value > 0
        for value in (ratio, baseline_ms, result_ms)
    )
    if not valid_measurements:
        errors.append("benchmark medians and throughput ratio must be finite and positive")
    elif not math.isclose(ratio, baseline_ms / result_ms, rel_tol=1e-9):
        errors.append("candidate throughput ratio does not match the saved medians")
    elif ratio < contract["minimum_throughput_ratio"]:
        errors.append("candidate throughput ratio is below the contract minimum")

    if comparison.get("gate_passed") is not True or comparison.get("throughput_ratio") != ratio:
        errors.append("comparison does not match the passing result")

    if errors:
        print("vector aggregate benchmark evidence failed")
        print("\n".join(f"- {error}" for error in errors))
        return 1

    print("vector aggregate benchmark evidence passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
