#!/usr/bin/env python3
"""Run one release suite and bind its result to the exact package tested."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEST_SUMMARY = re.compile(r"test result: .*?(\d+) passed; (\d+) failed; (\d+) ignored;")
POSTGRES_VERSION = re.compile(r"PGT_ACTUAL_POSTGRESQL_VERSION=([0-9]+\.[0-9]+(?:\.[0-9]+)?)")


def package_directory(artifact: Path, destination: Path) -> Path:
    extraction = destination / "archive"
    extraction.mkdir(parents=True)
    with tarfile.open(artifact, "r:gz") as archive:
        root = extraction.resolve()
        for member in archive.getmembers():
            target = (root / member.name).resolve()
            if not target.is_relative_to(root) or member.issym() or member.islnk() or member.isdev():
                raise ValueError(f"unsafe release archive member: {member.name!r}")
        archive.extractall(extraction)

    package = next(
        (
            path
            for path in extraction.iterdir()
            if path.is_dir() and (path / "lib").is_dir() and (path / "extension").is_dir()
        ),
        None,
    )
    if package is None:
        raise ValueError(f"{artifact} does not contain a pg_trickle package directory")

    result = destination / "candidate"
    shutil.copytree(package / "extension", result / "usr/share/postgresql/18/extension")
    shutil.copytree(package / "lib", result / "usr/lib/postgresql/18/lib")
    if not (result / "usr/share/postgresql/18/extension/pg_trickle.control").is_file():
        raise ValueError("release package is missing pg_trickle.control")
    if not list((result / "usr/lib/postgresql/18/lib").glob("pg_trickle.*")):
        raise ValueError("release package is missing the pg_trickle shared library")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qualification", type=Path, required=True)
    parser.add_argument("--suite", required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()

    try:
        args.log = args.log.resolve()
        args.result = args.result.resolve()
        if not args.log.is_relative_to(ROOT) or not args.result.is_relative_to(ROOT):
            raise ValueError("suite logs and results must stay inside the workspace")
        contract = json.loads(args.qualification.read_text(encoding="utf-8"))
        suite = next(item for item in contract["required_suites"] if item["id"] == args.suite)
        artifact = args.artifact.resolve()
        artifact_spec = next(item for item in contract["artifacts"] if item["id"] == suite["artifact_id"])
        artifact_digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        args.log.parent.mkdir(parents=True, exist_ok=True)
        args.result.parent.mkdir(parents=True, exist_ok=True)

        status = "failed"
        executed = 0
        skipped = 0
        measurement = None
        measurement_evidence = None
        postgres_version = None
        command = suite["command_argv"]
        if not isinstance(command, list) or not command or not all(isinstance(arg, str) for arg in command):
            raise ValueError(f"suite {args.suite!r} has no valid command_argv")

        with tempfile.TemporaryDirectory(prefix="pgt-release-suite-") as temp_name:
            temp = Path(temp_name)
            extension_dir = package_directory(artifact, temp)
            environment = os.environ.copy()
            environment.update(
                {
                    "PGT_EXTENSION_DIR": str(extension_dir),
                    "PGT_RELEASE_CANDIDATE_COMMIT": args.candidate_commit,
                    "PGT_RELEASE_ARTIFACT_SHA256": artifact_digest,
                    "PGT_DISABLE_NEXTEST": "1",
                }
            )
            if suite.get("e2e_image"):
                environment["PGS_E2E_IMAGE"] = suite["e2e_image"]
            measurement_path = None
            if suite.get("measurement_file"):
                measurement_path = (ROOT / suite["measurement_file"]).resolve()
                measurement_path.parent.mkdir(parents=True, exist_ok=True)
                measurement_path.unlink(missing_ok=True)
                environment["PGS_RELEASE_MEASUREMENT_JSON"] = str(measurement_path)

            completed = subprocess.run(command, cwd=ROOT, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            args.log.write_bytes(completed.stdout)
            sys.stdout.buffer.write(completed.stdout)
            summaries = TEST_SUMMARY.findall(completed.stdout.decode("utf-8", errors="replace"))
            if summaries:
                passed, failed, ignored = (sum(int(row[index]) for row in summaries) for index in range(3))
                executed = passed + failed
                skipped = ignored
            if suite.get("kind") == "check":
                executed = 1
            if suite.get("kind") == "measurement" and measurement_path and measurement_path.is_file():
                measurement = json.loads(measurement_path.read_text(encoding="utf-8"))
                executed = (
                    len(measurement.get("cases", []))
                    or len(measurement.get("budget_results", []))
                    or len(measurement.get("workloads", []))
                    or measurement.get("compared_benchmarks", 0)
                )

            version_match = POSTGRES_VERSION.search(completed.stdout.decode("utf-8", errors="replace"))
            postgres_version = version_match.group(1) if version_match else None
            if measurement is not None:
                measurement.update(
                    {
                        "candidate_commit": args.candidate_commit,
                        "artifact_digest": artifact_digest,
                        "postgresql_version": postgres_version,
                    }
                )
                measurement_path.write_text(json.dumps(measurement, indent=2) + "\n", encoding="utf-8")
                measurement_bytes = measurement_path.read_bytes()
                measurement_evidence = {
                    "path": measurement_path.relative_to(ROOT).as_posix(),
                    "bytes": len(measurement_bytes),
                    "sha256": hashlib.sha256(measurement_bytes).hexdigest(),
                    "attachments": [],
                }
                for attachment in measurement.get("raw_evidence_files", []):
                    attachment_path = (ROOT / attachment).resolve()
                    if not attachment_path.is_relative_to(ROOT) or not attachment_path.is_file():
                        raise ValueError(f"raw measurement attachment is missing: {attachment!r}")
                    attachment_bytes = attachment_path.read_bytes()
                    measurement_evidence["attachments"].append({
                        "path": attachment_path.relative_to(ROOT).as_posix(),
                        "bytes": len(attachment_bytes),
                        "sha256": hashlib.sha256(attachment_bytes).hexdigest(),
                    })
            status = "passed" if completed.returncode == 0 else "failed"
            if completed.returncode == 0 and suite.get("requires_postgresql", False) and postgres_version is None:
                status = "failed"
                print("ERROR: suite did not report the actual PostgreSQL server version", file=sys.stderr)
            if completed.returncode == 0 and suite.get("kind") == "test" and executed == 0:
                status = "failed"
                print("ERROR: required test suite executed zero tests", file=sys.stderr)
            if completed.returncode == 0 and suite.get("kind") == "measurement" and measurement is None:
                status = "failed"
                print("ERROR: measurement suite did not write its result file", file=sys.stderr)

        log_bytes = args.log.stat().st_size
        record = {
            "suite_id": suite["id"],
            "candidate_commit": args.candidate_commit,
            "artifact_id": artifact_spec["id"],
            "artifact_digest": artifact_digest,
            "platform": artifact_spec["platform"],
            "postgresql_version": postgres_version,
            "suite_version": suite["suite_version"],
            "command": suite["command"],
            "effective_workload": suite["workload"],
            "executed_tests": executed,
            "skipped_tests": skipped,
            "status": status,
            "log": {
                "path": args.log.relative_to(ROOT).as_posix(),
                "bytes": log_bytes,
                "sha256": hashlib.sha256(args.log.read_bytes()).hexdigest(),
            },
        }
        if measurement is not None:
            record["measurement"] = measurement
            record["measurement_evidence"] = measurement_evidence
        args.result.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        return 0 if status == "passed" else 1
    except (OSError, KeyError, StopIteration, ValueError, json.JSONDecodeError, tarfile.TarError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
