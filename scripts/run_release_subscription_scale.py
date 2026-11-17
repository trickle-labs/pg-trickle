#!/usr/bin/env python3
"""Measure exact-package distance subscriptions through one million source rows."""

from __future__ import annotations

import gzip
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ROW_COUNTS = (1_000, 10_000, 100_000, 1_000_000)
DIMENSIONS = 384
SUBSCRIPTIONS = (0, 1, 10, 100, 1000)
STATISTICS = ("unanalyzed", "analyzed")
ROUNDS = 1
PLAN_CASES = {
    (1_000_000, 0, "unanalyzed"),
    (1_000_000, 1_000, "unanalyzed"),
}
REPORT_MARKER = "PGVECTOR_SUBSCRIPTION_BENCHMARK_JSON="
TEST_COMMAND = [
    "bash",
    "scripts/run_e2e_tests.sh",
    "--test",
    "e2e_bench_tests",
    "--run-ignored",
    "ignored-only",
    "--no-capture",
    "-E",
    "test(test_pgvector_subscription_scaling_workload)",
]


def parse_case(
    output: str, rows: int, subscriptions: int, analyzed: bool, plan_capture: bool = False
) -> dict:
    reports = [line.removeprefix(REPORT_MARKER) for line in output.splitlines() if line.startswith(REPORT_MARKER)]
    if len(reports) != 1:
        raise ValueError("subscription benchmark must emit exactly one JSON result")
    report = json.loads(reports[0])
    if not isinstance(report, dict):
        raise ValueError("subscription benchmark result must be a JSON object")
    postgres = report.get("postgres", "")
    if (
        report.get("pg_trickle") != "0.109.0"
        or report.get("pgvector") != "0.8.7"
        or not isinstance(postgres, str)
        or not re.match(r"^PostgreSQL 18(?:\.|\s)", postgres)
        or report.get("dimensions") != DIMENSIONS
        or report.get("distinct_pivots") != 1
        or report.get("force_explicit_with_noop_trigger") is not True
        or report.get("nested_plan_capture") is not plan_capture
    ):
        raise ValueError("subscription benchmark used unexpected package, database, or plan-control settings")
    measurements = report.get("measurements")
    if not isinstance(measurements, list) or len(measurements) != 1:
        raise ValueError("subscription benchmark must contain exactly one isolated case")
    case = measurements[0]
    if (
        not isinstance(case, dict)
        or case.get("initial_rows") != rows
        or case.get("subscriptions") != subscriptions
        or case.get("analyzed") is not analyzed
    ):
        raise ValueError("subscription benchmark case does not match its requested scale")
    samples = case.get("samples")
    if not isinstance(samples, list) or len(samples) != ROUNDS:
        raise ValueError("subscription benchmark did not record the required round")
    sample = samples[0]
    if not isinstance(sample, dict) or sample.get("merge_strategy") != "explicit_dml":
        raise ValueError("subscription benchmark did not use the controlled merge strategy")
    for key in (
        "refresh_ms",
        "commit_ms",
        "stream_sequential_tuples_returned",
        "stream_index_tuples_fetched",
        "source_sequential_tuples_returned",
        "source_index_tuples_fetched",
    ):
        value = sample.get(key)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"subscription benchmark is missing a valid {key}")
    return report


def run_case(rows: int, subscriptions: int, statistics: str, plan_path: Path | None) -> dict:
    environment = os.environ.copy()
    environment.update(
        {
            "PGVECTOR_EXPECTED_VERSION": "0.8.7",
            "PGVECTOR_SUB_ROWS": str(rows),
            "PGVECTOR_SUB_COUNTS": str(subscriptions),
            "PGVECTOR_SUB_DIM": str(DIMENSIONS),
            "PGVECTOR_SUB_ROUNDS": str(ROUNDS),
            "PGVECTOR_SUB_STATISTICS": statistics,
            "PGVECTOR_SUB_FORCE_EXPLICIT": "1",
        }
    )
    environment.pop("PGVECTOR_SUB_PLAN_LOG", None)
    if plan_path is not None:
        environment["PGVECTOR_SUB_PLAN_LOG"] = str(plan_path)
    completed = subprocess.run(
        TEST_COMMAND,
        cwd=ROOT,
        env=environment,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    sys.stdout.write(completed.stdout)
    sys.stdout.flush()
    if completed.returncode != 0:
        raise RuntimeError(f"subscription case {rows}/{statistics}/{subscriptions} failed with exit {completed.returncode}")
    return parse_case(
        completed.stdout,
        rows,
        subscriptions,
        statistics == "analyzed",
        plan_capture=plan_path is not None,
    )


def validate_plan_log(output: str, rows: int, subscriptions: int, statistics: str) -> None:
    analyzed = statistics == "analyzed"
    case = f"rows={rows} subscriptions={subscriptions} analyzed={str(analyzed).lower()} round=0"
    if any(output.count(f"PGVECTOR_SUB_{stage} {case}") != 1 for stage in ("START", "END")):
        raise ValueError("subscription plan log is missing the expected case markers")
    if '"Plan":' not in output:
        raise ValueError("subscription plan log contains no auto_explain plan")


def archive_plan_log(path: Path, result: dict) -> str:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"subscription plan log was not written: {path.name}")
    compressed_path = path.with_suffix(path.suffix + ".gz")
    with path.open("rb") as source, gzip.open(compressed_path, "wb") as target:
        shutil.copyfileobj(source, target)
    path.unlink()
    relative_path = compressed_path.relative_to(ROOT).as_posix()
    result["raw_evidence_files"].append(relative_path)
    return relative_path


def attach_plan_log(path: Path, rows: int, subscriptions: int, statistics: str, result: dict) -> str:
    validate_plan_log(path.read_text(encoding="utf-8", errors="replace"), rows, subscriptions, statistics)
    return archive_plan_log(path, result)


def write_measurement(path: Path, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def host_info() -> dict:
    try:
        memory_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError):
        memory_bytes = None
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or None,
        "logical_cpus": os.cpu_count(),
        "memory_bytes": memory_bytes,
    }


def main() -> int:
    try:
        measurement_path = Path(os.environ["PGS_RELEASE_MEASUREMENT_JSON"]).resolve()
        if not measurement_path.is_relative_to(ROOT):
            raise ValueError("measurement output must stay inside the repository workspace")
        result = {
            "status": "running",
            "candidate_commit": os.environ["PGT_RELEASE_CANDIDATE_COMMIT"],
            "package_sha256": os.environ["PGT_RELEASE_ARTIFACT_SHA256"],
            "workload": "distance-subscription scale; not a RAG benchmark",
            "row_counts": list(ROW_COUNTS),
            "dimensions": DIMENSIONS,
            "subscription_counts": list(SUBSCRIPTIONS),
            "statistics": list(STATISTICS),
            "rounds_per_case": ROUNDS,
            "host": host_info(),
            "cases": [],
            "raw_evidence_files": [],
        }
        write_measurement(measurement_path, result)
        postgres_version = None
        for rows in ROW_COUNTS:
            for statistics in STATISTICS:
                for subscriptions in SUBSCRIPTIONS:
                    plan_path = None
                    case_id = (rows, subscriptions, statistics)
                    if case_id in PLAN_CASES:
                        plan_name = f"subscription-scale-{rows}-{subscriptions}-{statistics}.log"
                        plan_path = measurement_path.parent / plan_name
                        plan_path.unlink(missing_ok=True)
                    print(
                        f"=== Exact-package subscriptions: {rows} rows, {DIMENSIONS} dimensions, "
                        f"{subscriptions} subscriptions, {statistics} ===",
                        flush=True,
                    )
                    try:
                        report = run_case(rows, subscriptions, statistics, plan_path)
                    except (OSError, RuntimeError, ValueError):
                        if plan_path is not None and plan_path.is_file():
                            try:
                                archive_plan_log(plan_path, result)
                            except (OSError, ValueError) as archive_error:
                                print(
                                    f"WARNING: could not retain partial plan log {plan_path.name}: {archive_error}",
                                    file=sys.stderr,
                                    flush=True,
                                )
                        raise
                    postgres_version = report["postgres"]
                    case = {
                        "rows": rows,
                        "statistics": statistics,
                        "subscriptions": subscriptions,
                        "report": report,
                    }
                    if plan_path is not None:
                        plan_relative = attach_plan_log(
                            plan_path, rows, subscriptions, statistics, result
                        )
                        case["plan_log"] = plan_relative
                    result["cases"].append(case)
                    write_measurement(measurement_path, result)
        match = re.match(r"^PostgreSQL (18(?:\.\d+){1,2})", postgres_version or "")
        if not match:
            raise ValueError("subscription measurement did not report a PostgreSQL 18 version")
        result["status"] = "complete"
        write_measurement(measurement_path, result)
        print(f"PGT_ACTUAL_POSTGRESQL_VERSION={match.group(1)}", flush=True)
        return 0
    except (OSError, KeyError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        try:
            if "measurement_path" in locals() and "result" in locals():
                result["status"] = "failed"
                result["error"] = f"{type(error).__name__}: {error}"
                write_measurement(measurement_path, result)
        except OSError:
            pass
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
