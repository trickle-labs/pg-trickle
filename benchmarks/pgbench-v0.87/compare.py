#!/usr/bin/env python3
"""Validate v0.87 pgbench repetitions and apply the release budgets."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any


def finite_nonnegative(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def median(values: list[float]) -> float:
    if not values:
        raise ValueError("cannot calculate a median of an empty list")
    return float(statistics.median(values))


def fail(message: str) -> None:
    raise ValueError(message)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--budgets", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-repetitions", type=int, required=True)
    args = parser.parse_args()

    records = [json.loads(line) for line in args.raw.read_text(encoding="utf-8").splitlines() if line.strip()]
    budgets = json.loads(args.budgets.read_text(encoding="utf-8"))
    repetitions = args.expected_repetitions
    if len(records) != repetitions * 3:
        fail(f"expected {repetitions * 3} raw records, found {len(records)}")

    by_repetition: dict[int, dict[str, dict[str, Any]]] = {}
    for record in records:
        config = record.get("config")
        repetition = record.get("repetition")
        if config not in {"absent", "installed", "active"} or not isinstance(repetition, int):
            fail("raw record has an invalid configuration or repetition")
        if config in by_repetition.setdefault(repetition, {}):
            fail(f"duplicate raw record for repetition {repetition}, config {config}")
        by_repetition[repetition][config] = record
        for field in ("tps",):
            if not finite_nonnegative(record.get(field)):
                fail(f"{config}/{repetition}: invalid {field}")
        latency = record.get("transaction_latency_ms", {})
        for field in ("p50", "p95", "p99"):
            if not finite_nonnegative(latency.get(field)):
                fail(f"{config}/{repetition}: invalid latency {field}")
        if record.get("latency_sample_count", 0) <= 0:
            fail(f"{config}/{repetition}: no latency samples")
        if not record.get("correct", False):
            fail(f"{config}/{repetition}: correctness check failed")

    if set(by_repetition) != set(range(repetitions)):
        fail(f"repetitions must be exactly 0..{repetitions - 1}")

    ratios: list[dict[str, float]] = []
    for repetition in range(repetitions):
        row = by_repetition[repetition]
        if set(row) != {"absent", "installed", "active"}:
            fail(f"repetition {repetition} does not contain all three configurations")
        absent = row["absent"]
        installed = row["installed"]
        active = row["active"]
        if absent["tps"] <= 0 or absent["transaction_latency_ms"]["p99"] <= 0:
            fail(f"repetition {repetition}: absent baseline is not positive")
        if not active["cpu"]["supported"] or not finite_nonnegative(active["cpu"]["worker_share"]):
            fail(f"repetition {repetition}: active CPU share is unavailable")
        ratios.append(
            {
                "installed_no_st_tps_ratio": installed["tps"] / absent["tps"],
                "active_p99_latency_ratio": active["transaction_latency_ms"]["p99"] / absent["transaction_latency_ms"]["p99"],
                "refresh_cpu_share": active["cpu"]["worker_share"],
            }
        )

    medians = {key: median([row[key] for row in ratios]) for key in ratios[0]}
    product = budgets.get("product_budgets", {})
    required_budgets = (
        "installed_no_st_tps_ratio_min",
        "active_p99_latency_ratio_max",
        "refresh_cpu_share_max",
    )
    if any(not finite_nonnegative(product.get(key)) for key in required_budgets):
        fail("all product budgets must be finite, non-negative numbers")

    decisions = {
        "installed_no_st_tps_ratio": {
            "value": medians["installed_no_st_tps_ratio"],
            "budget": product["installed_no_st_tps_ratio_min"],
            "passed": medians["installed_no_st_tps_ratio"] >= product["installed_no_st_tps_ratio_min"],
        },
        "active_p99_latency_ratio": {
            "value": medians["active_p99_latency_ratio"],
            "budget": product["active_p99_latency_ratio_max"],
            "passed": medians["active_p99_latency_ratio"] <= product["active_p99_latency_ratio_max"],
        },
        "refresh_cpu_share": {
            "value": medians["refresh_cpu_share"],
            "budget": product["refresh_cpu_share_max"],
            "passed": medians["refresh_cpu_share"] <= product["refresh_cpu_share_max"],
        },
    }
    result = {
        "schema_version": 1,
        "version": budgets.get("version", "0.87.0"),
        "environment": {
            "commit": records[0].get("commit"),
            "postgres_version": records[0].get("postgres_version"),
            "images": {record["config"]: record.get("image") for record in records[:3]},
        },
        "raw_repetitions": records,
        "per_repetition": ratios,
        "medians": medians,
        "decisions": decisions,
        "passed": all(decision["passed"] for decision in decisions.values()),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"medians": medians, "passed": result["passed"]}, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"pgbench v0.87 gate failed: {error}")
        raise SystemExit(1)
