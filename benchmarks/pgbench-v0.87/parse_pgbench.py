#!/usr/bin/env python3
"""Convert one pgbench run and its Linux CPU samples into JSON."""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path


TPS_RE = re.compile(r"tps\s*=\s*([0-9]+(?:\.[0-9]+)?)")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def read_tps(stdout_path: Path) -> float:
    matches = TPS_RE.findall(stdout_path.read_text(encoding="utf-8"))
    if not matches:
        raise ValueError(f"pgbench TPS line missing in {stdout_path}")
    return float(matches[-1])


def read_latencies(log_dir: Path) -> list[float]:
    values: list[float] = []
    # PostgreSQL 18's pgbench uses the log prefix plus the backend PID and
    # does not append a .log suffix.
    for path in sorted(path for path in log_dir.iterdir() if path.is_file()):
        for line in path.read_text(encoding="utf-8").splitlines():
            fields = line.split()
            if len(fields) < 3:
                continue
            try:
                latency_us = float(fields[2])
            except ValueError:
                continue
            if math.isfinite(latency_us) and latency_us >= 0:
                values.append(latency_us / 1000.0)
    if not values:
        raise ValueError(f"sampled pgbench latency logs are empty in {log_dir}")
    return values


def read_cpu_sample(value: str) -> tuple[int, int] | None:
    if value == "unsupported":
        return None
    worker, total = (int(part) for part in value.split(",", 1))
    if worker < 0 or total < 0:
        return None
    return worker, total


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--repetition", type=int, required=True)
    parser.add_argument("--stdout", type=Path, required=True)
    parser.add_argument("--log-dir", type=Path, required=True)
    parser.add_argument("--cpu-before", required=True)
    parser.add_argument("--cpu-after", required=True)
    parser.add_argument("--refresh-count", type=int, required=True)
    parser.add_argument("--refresh-duration-ms", type=float, required=True)
    parser.add_argument("--correct", choices=("true", "false"), required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--postgres-version", required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()

    latencies = read_latencies(args.log_dir)
    before = read_cpu_sample(args.cpu_before)
    after = read_cpu_sample(args.cpu_after)
    cpu_supported = before is not None and after is not None
    cpu_share = None
    if cpu_supported:
        worker_delta = after[0] - before[0]
        total_delta = after[1] - before[1]
        if worker_delta < 0 or total_delta <= 0 or worker_delta > total_delta:
            cpu_supported = False
        else:
            cpu_share = worker_delta / total_delta

    result = {
        "config": args.config,
        "repetition": args.repetition,
        "commit": args.commit,
        "postgres_version": args.postgres_version,
        "image": args.image,
        "tps": read_tps(args.stdout),
        "transaction_latency_ms": {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
        },
        "latency_sample_count": len(latencies),
        "refresh": {
            "count": args.refresh_count,
            "duration_ms": args.refresh_duration_ms,
        },
        "correct": args.correct == "true",
        "cpu": {
            "supported": cpu_supported,
            "worker_share": cpu_share,
            "worker_jiffies": None if not cpu_supported else after[0] - before[0],
            "postgres_jiffies": None if not cpu_supported else after[1] - before[1],
        },
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
