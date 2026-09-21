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


def worker_ticks(path: Path) -> int:
    ticks = 0
    last_seen: dict[tuple[int, int], int] = {}
    sample = -1
    for line in path.read_text(encoding="utf-8").splitlines():
        if line == "S":
            sample += 1
            continue
        kind, pid, start, current = line.split()
        if kind != "W" or sample < 0:
            raise ValueError(f"invalid CPU sample: {line}")
        key = (int(pid), int(start))
        current_ticks = int(current)
        previous = last_seen.get(key, current_ticks if sample == 0 else 0)
        if current_ticks < previous:
            raise ValueError(f"CPU ticks decreased for PID {pid}")
        ticks += current_ticks - previous
        last_seen[key] = current_ticks
    if sample < 1:
        raise ValueError("CPU sampler recorded fewer than two snapshots")
    return ticks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--repetition", type=int, required=True)
    parser.add_argument("--stdout", type=Path, required=True)
    parser.add_argument("--log-dir", type=Path, required=True)
    parser.add_argument("--cpu-before", required=True)
    parser.add_argument("--cpu-after", required=True)
    parser.add_argument("--cpu-samples", type=Path, required=True)
    parser.add_argument("--cpu-hz", type=int, required=True)
    parser.add_argument("--refresh-count", type=int, required=True)
    parser.add_argument("--refresh-duration-ms", type=float, required=True)
    parser.add_argument("--correct", choices=("true", "false"), required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--postgres-version", required=True)
    parser.add_argument("--postgres-settings", required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()

    latencies = read_latencies(args.log_dir)
    worker_jiffies = worker_ticks(args.cpu_samples)
    if args.config == "active" and worker_jiffies == 0:
        raise ValueError("active run recorded no pg_trickle worker CPU")
    container_cpu_us = int(args.cpu_after) - int(args.cpu_before)
    if args.cpu_hz <= 0 or container_cpu_us <= 0:
        raise ValueError("invalid container CPU sample")
    worker_share = worker_jiffies * 1_000_000 / (args.cpu_hz * container_cpu_us)
    if not 0 <= worker_share <= 1:
        raise ValueError("worker CPU exceeds container CPU")

    result = {
        "config": args.config,
        "repetition": args.repetition,
        "commit": args.commit,
        "postgres_version": args.postgres_version,
        "postgres_settings": args.postgres_settings,
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
            "supported": True,
            "worker_share": worker_share,
            "worker_jiffies": worker_jiffies,
            "container_cpu_us": container_cpu_us,
        },
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
