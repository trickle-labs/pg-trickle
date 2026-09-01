#!/usr/bin/env python3
"""Offline release gate for the v0.89.0 window admission contract."""

from __future__ import annotations

import json
import math
import re
import statistics
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
EXPECTED_FUNCTIONS = {
    "row_number",
    "rank",
    "dense_rank",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
    "sum",
    "count",
}
ROW_NUMBER_SCOPE = (
    "one built-in ROW_NUMBER over a direct keyed scan with exact same-name "
    "projections and every non-null identity column in ORDER BY"
)
EXPECTED_MEASUREMENT_CELLS = {
    (partition_rows, shape)
    for partition_rows in (1_000, 10_000, 100_000)
    for shape in ("tail_insert", "front_insert")
}


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def main() -> int:
    errors: list[str] = []
    cargo = read("Cargo.toml")
    version = re.search(r'^version\s*=\s*"([^"]+)"', cargo, re.MULTILINE)
    if not version or version.group(1) != "0.89.0":
        errors.append("Cargo.toml version must be 0.89.0")

    for path in (
        "sql/pg_trickle--0.88.0--0.89.0.sql",
        "src/dvm/parser/window_plan.rs",
        "src/window_state.rs",
        "docs/DVM_SUPPORT_MATRIX.md",
        "benchmarks/window-v0.89/admission.json",
    ):
        if not (ROOT / path).exists():
            errors.append(f"missing v0.89 artifact: {path}")

    try:
        admission = json.loads(read("benchmarks/window-v0.89/admission.json"))
    except (OSError, json.JSONDecodeError) as error:
        errors.append(f"invalid v0.89 admission artifact: {error}")
        admission = {}

    if admission.get("release") != "0.89.0":
        errors.append("window admission release must be 0.89.0")
    if admission.get("minimum_measured_improvement") != 0.2:
        errors.append("window admission improvement gate must be 20 percent")
    if admission.get("performance_measurements_published") is not True:
        errors.append("v0.89 must publish the negative window measurements")
    if admission.get("raw_samples") != "samples.json":
        errors.append("v0.89 admission must reference samples.json")
    if admission.get("speedup_claim") is not None:
        errors.append("v0.89 must not claim a window speedup")
    if admission.get("admission_result") != "rejected":
        errors.append("v0.89 window runtime admission must be rejected")

    measurements = admission.get("measurements", {})
    if not isinstance(measurements, dict):
        errors.append("window measurements must be an object")
        measurements = {}
    row_number_measurements = measurements.get("row_number", [])
    if not isinstance(row_number_measurements, list):
        errors.append("row_number measurements must be a list")
        row_number_measurements = []
    if measurements.get("status") != "published_negative":
        errors.append("window benchmark measurements must publish the rejection")

    try:
        samples = json.loads(read("benchmarks/window-v0.89/samples.json"))
    except (OSError, json.JSONDecodeError) as error:
        errors.append(f"invalid v0.89 raw samples: {error}")
        samples = []
    if not isinstance(samples, list):
        errors.append("window raw samples must be an array")
        samples = []
    measurement_cells = {
        (entry.get("partition_rows"), entry.get("shape"))
        for entry in row_number_measurements
        if isinstance(entry, dict)
    }
    if (
        measurement_cells != EXPECTED_MEASUREMENT_CELLS
        or len(row_number_measurements) != len(EXPECTED_MEASUREMENT_CELLS)
    ):
        errors.append("row_number measurements must cover the six benchmark cells")
    for entry in row_number_measurements:
        if not isinstance(entry, dict):
            errors.append("row_number measurement entries must be objects")
            continue
        cell = f"{entry.get('partition_rows')}/{entry.get('shape')}"
        number_fields = (
            "state_candidate_median_ms",
            "partition_recompute_median_ms",
            "recompute_to_candidate_ratio",
            "state_candidate_median_wal_bytes",
            "partition_recompute_median_wal_bytes",
            "state_candidate_median_output_rows",
            "partition_recompute_median_output_rows",
        )
        if any(
            isinstance(entry.get(field), bool)
            or not isinstance(entry.get(field), (int, float))
            or not math.isfinite(entry[field])
            for field in number_fields
        ):
            errors.append(f"row_number {cell} must contain finite measured values")
            continue
        for prefix in ("state_candidate", "partition_recompute"):
            bounds = entry.get(f"{prefix}_range_ms")
            median = entry[f"{prefix}_median_ms"]
            if (
                not isinstance(bounds, list)
                or len(bounds) != 2
                or any(
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    for value in bounds
                )
                or not bounds[0] <= median <= bounds[1]
            ):
                errors.append(f"row_number {cell} {prefix} range must contain its median")
        ratio = entry["recompute_to_candidate_ratio"]
        measured_ratio = (
            entry["partition_recompute_median_ms"] / entry["state_candidate_median_ms"]
        )
        if abs(ratio - measured_ratio) > 0.0002:
            errors.append(f"row_number {cell} ratio does not match its medians")
        if ratio >= 1.0:
            errors.append(f"row_number {cell} must preserve the measured rejection")

        cell_samples = [
            sample
            for sample in samples
            if isinstance(sample, dict)
            and sample.get("partition_rows") == entry.get("partition_rows")
            and sample.get("shape") == entry.get("shape")
        ]
        for strategy, prefix in (
            ("state_backed", "state_candidate"),
            ("partition_recompute", "partition_recompute"),
        ):
            strategy_samples = [
                sample for sample in cell_samples if sample.get("strategy") == strategy
            ]
            if len(strategy_samples) != 5 or {
                sample.get("repetition") for sample in strategy_samples
            } != set(range(1, 6)):
                errors.append(f"row_number {cell} {strategy} must have repetitions 1-5")
                continue
            for field in ("elapsed_ms", "wal_bytes", "output_rows"):
                if any(
                    isinstance(sample.get(field), bool)
                    or not isinstance(sample.get(field), (int, float))
                    or not math.isfinite(sample[field])
                    for sample in strategy_samples
                ):
                    errors.append(f"row_number {cell} {strategy} has invalid {field}")
                    continue
            elapsed = [sample["elapsed_ms"] for sample in strategy_samples]
            expected = {
                f"{prefix}_median_ms": statistics.median(elapsed),
                f"{prefix}_range_ms": [min(elapsed), max(elapsed)],
                f"{prefix}_median_wal_bytes": statistics.median(
                    sample["wal_bytes"] for sample in strategy_samples
                ),
                f"{prefix}_median_output_rows": statistics.median(
                    sample["output_rows"] for sample in strategy_samples
                ),
            }
            for field, value in expected.items():
                recorded = entry.get(field)
                if isinstance(value, list):
                    matches = isinstance(recorded, list) and len(recorded) == 2 and all(
                        math.isclose(actual, wanted, abs_tol=0.000001)
                        for actual, wanted in zip(recorded, value, strict=True)
                    )
                else:
                    matches = isinstance(recorded, (int, float)) and math.isclose(
                        recorded, value, abs_tol=0.000001
                    )
                if not matches:
                    errors.append(f"row_number {cell} {field} does not match samples.json")

    if len(samples) != 60:
        errors.append("window raw sample artifact must contain exactly 60 samples")

    functions = admission.get("functions", [])
    names = {entry.get("name") for entry in functions if isinstance(entry, dict)}
    if names != EXPECTED_FUNCTIONS or len(functions) != len(EXPECTED_FUNCTIONS):
        errors.append("window admission artifact must cover every v0.89 candidate")
    for entry in functions:
        if not isinstance(entry, dict):
            errors.append("window admission function entries must be objects")
            continue
        name = entry.get("name")
        if name == "row_number":
            if entry.get("scope") != ROW_NUMBER_SCOPE:
                errors.append("row_number must retain the narrow v0.89 scope")
        if entry.get("runtime_enabled") is not False:
            errors.append(f"{name} must remain runtime-disabled")
        if entry.get("strategy") != "partition_recompute":
            errors.append(f"{name} must use partition recomputation")
        expected_reason = (
            "WINDOW_RECOMPUTE_CHEAPER"
            if name == "row_number"
            else "WINDOW_INCREMENTAL_UNIMPLEMENTED"
        )
        if entry.get("reason") != expected_reason:
            errors.append(f"{name} must use {expected_reason}")

    source = "\n".join(
        read(path)
        for path in (
            "src/dvm/parser/types.rs",
            "src/dvm/parser/window_plan.rs",
            "src/dvm/planner.rs",
            "src/window_state.rs",
        )
        if (ROOT / path).exists()
    )
    for symbol in (
        "WindowStrategyPlan",
        "runtime_enabled = false",
        "WindowIncrementalStrategy::PartitionRecompute",
        "WINDOW_COST_MODEL_VERSION",
        "pgt_window_states",
    ):
        if symbol not in source:
            errors.append(f"missing window admission contract: {symbol}")

    docs = read("docs/DVM_SUPPORT_MATRIX.md")
    if "no window family is runtime-enabled" not in docs.lower():
        errors.append("support matrix must disclose that runtime admission was rejected")

    if errors:
        print("v0_89_release_gate: FAILED")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("v0_89_release_gate: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
