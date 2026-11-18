#!/usr/bin/env python3
"""Measure centroid strategy costs against the exact release package."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIMENSIONS = (384, 768, 1536)
REWRITES = (1, 2500)
ROWS = 5000
SAMPLES = 20
WARMUP = 3
MARKER = "PGVECTOR_BENCHMARK_JSON="


def parse_measurement(output: str, dimensions: int) -> dict:
    records = [line.partition(MARKER)[2] for line in output.splitlines() if MARKER in line]
    if len(records) != 1:
        raise ValueError(f"expected one {MARKER} result, found {len(records)}")
    report = json.loads(records[0])
    repeated = report.get("centroid_repeated", {})
    if report.get("dimensions") != dimensions or report.get("rows") != ROWS:
        raise ValueError("benchmark output does not match the requested dimensions and row count")
    if repeated.get("measured_rounds") != SAMPLES or repeated.get("warmup_rounds") != WARMUP:
        raise ValueError("benchmark output has the wrong warmup or sample count")
    if repeated.get("changed_rows_per_round") not in REWRITES:
        raise ValueError("benchmark output has an unexpected rewrite size")
    if len(repeated.get("observations", [])) != 4 * 3 * SAMPLES:
        raise ValueError("benchmark output is missing centroid observations")
    return report


class DockerStatsSampler:
    def __init__(self, path: Path):
        self.path = path
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.samples = 0

    def start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")
        self.thread.start()

    def stop(self) -> None:
        self.stopped.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=15)

    def _sample(self) -> None:
        with self.path.open("a", encoding="utf-8") as output:
            while not self.stopped.is_set():
                listed = subprocess.run(
                    ["docker", "ps", "--filter", "label=com.pgtrickle.repo=pg-stream", "--format", "{{.ID}}"],
                    check=False,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                ids = listed.stdout.splitlines() if listed.returncode == 0 else []
                if listed.returncode != 0:
                    output.write(json.dumps({"error": listed.stderr.strip()}) + "\n")
                for container_id in ids:
                    result = subprocess.run(
                        ["docker", "stats", "--no-stream", "--format", "{{json .}}", container_id],
                        check=False,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    record = {
                        "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
                        "container_id": container_id,
                        "stats": result.stdout.strip() if result.returncode == 0 else None,
                        "error": result.stderr.strip() if result.returncode != 0 else None,
                    }
                    output.write(json.dumps(record, sort_keys=True) + "\n")
                    output.flush()
                    self.samples += result.returncode == 0
                self.stopped.wait(1)


def run_case(dimensions: int, changed_rows: int, measurement_path: Path) -> dict:
    stem = f"centroid-cost-{dimensions}-batch-{changed_rows}"
    log_path = measurement_path.with_name(f"{stem}.log")
    stats_path = measurement_path.with_name(f"{stem}-docker-stats.jsonl")
    log_path.unlink(missing_ok=True)
    stats_path.unlink(missing_ok=True)
    environment = os.environ.copy()
    environment.update(
        {
            "PGT_E2E_MUTATION_PACKAGE_DIR": environment["PGT_EXTENSION_DIR"],
            "PGVECTOR_BENCH_DIM": str(dimensions),
            "PGVECTOR_BENCH_ROWS": str(ROWS),
            "PGVECTOR_BENCH_SAMPLES": "1",
            "PGVECTOR_CENTROID_SAMPLES": str(SAMPLES),
            "PGVECTOR_CENTROID_REWRITE_ROWS": str(changed_rows),
            "PGVECTOR_EXPECTED_VERSION": "0.8.7",
        }
    )
    command = [
        "bash",
        "scripts/run_e2e_tests.sh",
        "--retries",
        "0",
        "--no-capture",
        "--run-ignored",
        "all",
        "--test",
        "e2e_bench_tests",
        "-E",
        "test(test_pgvector_filtered_ann_workload)",
    ]
    sampler = DockerStatsSampler(stats_path)
    sampler.start()
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=environment,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    finally:
        sampler.stop()
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(f"{stem} test failed; see {log_path}:\n{completed.stdout[-4000:]}")
    report = parse_measurement(completed.stdout, dimensions)
    if report.get("pgvector") != "0.8.7" or not report.get("postgres", "").startswith("PostgreSQL 18."):
        raise ValueError(f"{stem} ran against an unsupported database image")
    if sampler.samples == 0:
        raise RuntimeError(f"{stem} produced no Docker CPU/memory samples; see {stats_path}")
    version = re.search(r"PostgreSQL ([0-9]+\.[0-9]+)", report["postgres"])
    if version:
        print(f"PGT_ACTUAL_POSTGRESQL_VERSION={version.group(1)}", flush=True)
    print(
        f"Centroid cost case {dimensions}D / {changed_rows} changed rows passed; "
        f"{sampler.samples} resource samples retained at {stats_path.relative_to(ROOT)}",
        flush=True,
    )
    return {
        "dimensions": dimensions,
        "rows": ROWS,
        "changed_rows_per_round": changed_rows,
        "measurement": report,
        "resource_samples": sampler.samples,
        "resource_file": stats_path.relative_to(ROOT).as_posix(),
        "test_log": log_path.relative_to(ROOT).as_posix(),
    }


def write_measurement(path: Path, report: dict) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    measurement_path = Path(os.environ["PGS_RELEASE_MEASUREMENT_JSON"]).resolve()
    if not measurement_path.is_relative_to(ROOT):
        raise ValueError("measurement output must stay inside the repository workspace")
    cases: list[dict] = []
    evidence: list[Path] = []
    result = {
        "status": "running",
        "rows": ROWS,
        "dimensions": list(DIMENSIONS),
        "warmup_rounds": WARMUP,
        "measured_rounds": SAMPLES,
        "workloads": cases,
        "cases": cases,
        "raw_evidence_files": [],
    }
    write_measurement(measurement_path, result)
    try:
        for dimensions in DIMENSIONS:
            for changed_rows in REWRITES:
                print(
                    f"=== Exact-package centroid cost: {dimensions} dimensions, "
                    f"{changed_rows} changed rows ===",
                    flush=True,
                )
                stem = f"centroid-cost-{dimensions}-batch-{changed_rows}"
                evidence.extend(
                    [
                        measurement_path.with_name(f"{stem}.log"),
                        measurement_path.with_name(f"{stem}-docker-stats.jsonl"),
                    ]
                )
                case = run_case(dimensions, changed_rows, measurement_path)
                cases.append(case)
                result["raw_evidence_files"] = sorted(
                    path.relative_to(ROOT).as_posix() for path in evidence if path.is_file()
                )
                write_measurement(measurement_path, result)
        result["status"] = "complete"
        result["candidate_commit"] = os.environ["PGT_RELEASE_CANDIDATE_COMMIT"]
        result["artifact_sha256"] = os.environ["PGT_RELEASE_ARTIFACT_SHA256"]
        result["workload"] = "5,000-row centroid comparison across 384/768/1536 dimensions, four group shapes, warmups, repeated one-row and group-scale rewrites"
        write_measurement(measurement_path, result)
        return 0
    except Exception as error:
        result["status"] = "failed"
        result["error"] = f"{type(error).__name__}: {error}"
        result["raw_evidence_files"] = sorted(path.relative_to(ROOT).as_posix() for path in evidence if path.is_file())
        write_measurement(measurement_path, result)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
