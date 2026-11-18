#!/usr/bin/env python3
"""Measure the capped live RAG workload against the exact release package."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import run_live_rag_batches


ROOT = Path(__file__).resolve().parents[2]
ROWS = (10_000, 100_000)
DIMENSIONS = (384, 768, 1536)
BATCH = 1000
CYCLES = 3
DELAY_SECONDS = 0.02
DELAY_SEED = 1396
TENANT = 2
POSTGRES_PASSWORD = "release-qualification"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--rows", type=int, choices=ROWS, nargs="+", required=True)
    result.add_argument("--dimensions", type=int, choices=DIMENSIONS, nargs="+", required=True)
    return result


def docker(*args: str, capture_output: bool = False) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip() if capture_output else ""


class DockerStats:
    def __init__(self, container: str, path: Path):
        self.container = container
        self.path = path
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")
        self.thread.start()

    def stop(self) -> None:
        self.stopped.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=20)

    def _sample(self) -> None:
        with self.path.open("a", encoding="utf-8") as output:
            while not self.stopped.is_set():
                result = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{json .}}", self.container],
                    check=False,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                sample = {
                    "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
                    "stats": result.stdout.strip() if result.returncode == 0 else None,
                    "error": result.stderr.strip() if result.returncode != 0 else None,
                }
                output.write(json.dumps(sample, sort_keys=True) + "\n")
                output.flush()
                if self.stopped.wait(15):
                    break


def wait_for_database(container: str) -> None:
    for _ in range(90):
        result = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", "postgres", "-d", "postgres"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            return
        time.sleep(2)
    logs = subprocess.run(["docker", "logs", container], check=False, text=True, capture_output=True)
    raise RuntimeError(f"PostgreSQL did not become ready: {logs.stderr[-2000:]} {logs.stdout[-2000:]}")


def start_database(image: str, container: str) -> int:
    try:
        docker(
            "run",
            "--detach",
            "--name",
            container,
            "--shm-size",
            "2g",
            "--publish",
            "127.0.0.1::5432",
            "--env",
            f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
            image,
        )
        wait_for_database(container)
        port = docker("port", container, "5432/tcp", capture_output=True).rsplit(":", 1)[-1]
        return int(port)
    except Exception:
        subprocess.run(["docker", "rm", "-f", container], check=False, stdout=subprocess.DEVNULL)
        raise


def connection_environment(port: int) -> dict[str, str]:
    return {
        "PGHOST": "127.0.0.1",
        "PGPORT": str(port),
        "PGUSER": "postgres",
        "PGPASSWORD": POSTGRES_PASSWORD,
        "PGDATABASE": "postgres",
    }


def run_case(rows: int, dimensions: int, measurement_path: Path) -> dict:
    candidate_commit = os.environ["PGT_RELEASE_CANDIDATE_COMMIT"]
    package_sha256 = os.environ["PGT_RELEASE_ARTIFACT_SHA256"]
    candidate_archive = Path(os.environ["PGT_RELEASE_CANDIDATE_ARCHIVE"])
    candidate_root = os.environ["PGT_EXTENSION_DIR"]
    attestation_path = os.environ["PGT_RELEASE_ATTESTATION_PATH"]
    image = os.environ["PGS_E2E_IMAGE"]
    container = f"pgt-live-rag-{os.getpid()}-{rows}-{dimensions}"
    port = start_database(image, container)
    size = f"{rows // 1000}k"
    stats_path = measurement_path.with_name(f"live-rag-{size}-{dimensions}-docker-stats.jsonl")
    checkpoint_path = measurement_path.with_name(f"live-rag-{size}-{dimensions}-checkpoint.json")
    stats = DockerStats(container, stats_path)
    connection_vars = connection_environment(port)
    previous = {key: os.environ.get(key) for key in connection_vars}
    os.environ.update(connection_vars)
    try:
        version = subprocess.run(
            ["psql", "-X", "-w", "-qAt", "-c", "SELECT split_part(current_setting('server_version'), ' ', 1)"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
        print(f"PGT_ACTUAL_POSTGRESQL_VERSION={version}", flush=True)
        subprocess.run(
            [
                sys.executable,
                "scripts/release_install_attestation.py",
                "--container",
                container,
                "--candidate-root",
                candidate_root,
                "--output",
                attestation_path,
            ],
            check=True,
            env=os.environ.copy(),
        )
        stats.start()
        args = argparse.Namespace(
            rows=rows,
            dimensions=dimensions,
            batch=BATCH,
            cycles=CYCLES,
            delay_seconds=DELAY_SECONDS,
            delay_profile="uniform",
            delay_seed=DELAY_SEED,
            tenant=TENANT,
            workload="mixed",
            concurrent_readers=1,
            candidate_commit=candidate_commit,
            package_sha256=package_sha256,
            candidate_archive=candidate_archive,
            attest_container=container,
            checkpoint=checkpoint_path,
        )
        report = run_live_rag_batches.run(args)
        report["docker_stats_file"] = stats_path.relative_to(ROOT).as_posix()
        report["checkpoint_file"] = checkpoint_path.relative_to(ROOT).as_posix()
        return report
    finally:
        stats.stop()
        subprocess.run(["docker", "rm", "-f", container], check=False, stdout=subprocess.DEVNULL)
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def write_measurement(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    args = parser().parse_args()
    if tuple(args.rows) != ROWS or tuple(args.dimensions) != DIMENSIONS:
        parser().error(f"release matrix requires rows {ROWS} and dimensions {DIMENSIONS} in order")
    measurement_path = Path(os.environ["PGS_RELEASE_MEASUREMENT_JSON"]).resolve()
    if not measurement_path.is_relative_to(ROOT):
        parser().error("measurement output must stay inside the repository workspace")
    reports: list[dict] = []
    evidence: list[Path] = []
    for rows in args.rows:
        size = f"{rows // 1000}k"
        for dimensions in args.dimensions:
            evidence.extend(
                [
                    measurement_path.with_name(f"live-rag-{size}-{dimensions}-docker-stats.jsonl"),
                    measurement_path.with_name(f"live-rag-{size}-{dimensions}-checkpoint.json"),
                ]
            )
    for path in evidence:
        path.unlink(missing_ok=True)
    result = {
        "status": "running",
        "rows": list(args.rows),
        "dimensions": list(args.dimensions),
        "cases": reports,
        "raw_evidence_files": [],
    }
    write_measurement(measurement_path, result)
    try:
        for rows in args.rows:
            for dimensions in args.dimensions:
                print(f"=== Exact-package live RAG: {rows} rows, {dimensions} dimensions ===", flush=True)
                report = run_case(rows, dimensions, measurement_path)
                reports.append(report)
                result["raw_evidence_files"] = sorted(
                    path.relative_to(ROOT).as_posix() for path in evidence if path.is_file()
                )
                write_measurement(measurement_path, result)
        result["status"] = "complete"
        result["candidate_commit"] = os.environ["PGT_RELEASE_CANDIDATE_COMMIT"]
        result["package_sha256"] = os.environ["PGT_RELEASE_ARTIFACT_SHA256"]
        result["workload"] = "10,000/100,000-document mixed update/insert/delete corpus, 384/768/1536 dimensions"
        result["raw_evidence_files"] = sorted(
            path.relative_to(ROOT).as_posix() for path in evidence if path.is_file()
        )
        write_measurement(measurement_path, result)
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    except Exception as error:
        result["status"] = "failed"
        result["error"] = f"{type(error).__name__}: {error}"
        result["raw_evidence_files"] = sorted(
            path.relative_to(ROOT).as_posix() for path in evidence if path.is_file()
        )
        write_measurement(measurement_path, result)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
