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
import time
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEST_SUMMARY = re.compile(r"test result: .*?(\d+) passed; (\d+) failed; (\d+) ignored;")
POSTGRES_VERSION = re.compile(r"PGT_ACTUAL_POSTGRESQL_VERSION=([0-9]+\.[0-9]+(?:\.[0-9]+)?)")
MACHINE_STATUS = {"ok": "passed", "failed": "failed", "leaked": "failed", "timeout": "failed", "ignored": "skipped"}


def canonical_digest(value: object) -> str:
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def payload_manifest(candidate: Path) -> dict[str, object]:
    files: list[dict[str, object]] = []
    for root_name in ("usr/lib/postgresql/18/lib", "usr/share/postgresql/18/extension"):
        root = candidate / root_name
        for path in sorted(root.rglob("*")):
            if path.is_symlink() or not path.is_file():
                continue
            data = path.read_bytes()
            files.append(
                {
                    "path": path.relative_to(candidate).as_posix(),
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
    if not files:
        raise ValueError("candidate package contains no installed payload files")
    return {"files": files, "sha256": canonical_digest(files)}


def artifact_identity(artifact: Path) -> dict[str, object] | None:
    try:
        if artifact.suffix == ".zip":
            with zipfile.ZipFile(artifact) as archive:
                names = [name for name in archive.namelist() if name.endswith("/release-candidate.json")]
                if len(names) != 1:
                    return None
                return json.loads(archive.read(names[0]))
        with tarfile.open(artifact, "r:gz") as archive:
            members = [member for member in archive.getmembers() if member.name.endswith("/release-candidate.json")]
            if len(members) != 1:
                return None
            stream = archive.extractfile(members[0])
            if stream is None:
                return None
            return json.loads(stream.read())
    except (tarfile.TarError, zipfile.BadZipFile):
        return None
    except (OSError, KeyError, json.JSONDecodeError) as error:
        raise ValueError(f"could not read release candidate identity from {artifact}: {error}") from error


def machine_case_attempts(output: str, required_cases: list[str]) -> list[list[dict[str, str]]]:
    events: dict[str, list[str]] = {identity: [] for identity in required_cases}
    for line in output.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("type") != "test" or message.get("event") not in MACHINE_STATUS:
            continue
        name = message.get("name")
        if not isinstance(name, str):
            continue
        for identity in required_cases:
            binary, test_name = identity.split("::", 1)
            if name.endswith(f"::{binary}${test_name}"):
                events[identity].append(MACHINE_STATUS[message["event"]])
                break
    attempt_count = max((len(statuses) for statuses in events.values()), default=0)
    return [
        [
            {"id": identity, "status": statuses[index] if index < len(statuses) else "missing"}
            for identity, statuses in events.items()
        ]
        for index in range(attempt_count)
    ]


def machine_summary(output: str) -> tuple[int, int, int] | None:
    summary = [
        json.loads(line)
        for line in output.splitlines()
        if line.startswith("{") and '"type":"suite"' in line
    ]
    completed = [item for item in summary if item.get("event") in {"ok", "failed"}]
    if not completed:
        return None
    return (
        sum(int(item.get("passed", 0)) for item in completed),
        sum(int(item.get("failed", 0)) for item in completed),
        sum(int(item.get("ignored", 0)) for item in completed),
    )


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
        declared_identity = artifact_identity(artifact)
        identity_required = bool(contract.get("candidate_identity", {}).get("required"))
        if declared_identity is not None and not isinstance(declared_identity, dict):
            raise ValueError("release artifact candidate identity must be a JSON object")
        if identity_required and declared_identity is None:
            raise ValueError("release artifact has no candidate identity")
        if declared_identity is not None:
            for key, expected in (
                ("candidate_commit", args.candidate_commit),
                ("build_kind", contract.get("build_kind", "exact-release")),
                ("feature_scope", contract.get("feature_scope", "default-release")),
            ):
                if declared_identity.get(key) != expected:
                    raise ValueError(f"release artifact candidate identity has the wrong {key}")
            if declared_identity.get("build_provenance") != contract.get("build_provenance"):
                raise ValueError("release artifact candidate identity has untrusted build provenance")
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
        required_cases = suite.get("required_cases", [])
        if not isinstance(required_cases, list) or not all(isinstance(case, str) for case in required_cases):
            raise ValueError(f"suite {args.suite!r} has invalid required_cases")
        if len(required_cases) != len(set(required_cases)):
            raise ValueError(f"suite {args.suite!r} has duplicate required cases")
        shard = suite.get("shard", {"id": suite["id"], "index": 1, "count": 1})
        if not isinstance(shard, dict):
            raise ValueError(f"suite {args.suite!r} has invalid shard metadata")
        build_kind = (declared_identity or {}).get("build_kind", contract.get("build_kind", "unknown"))
        feature_scope = (declared_identity or {}).get(
            "feature_scope", contract.get("feature_scope", "default-release")
        )
        candidate_manifest: dict[str, object] = {}
        installation_observations: list[dict[str, object]] = []
        observed_case_results: list[dict[str, str]] = []
        attempt_case_results: list[list[dict[str, str]]] = []
        attempt_started_at = time.time()

        with tempfile.TemporaryDirectory(prefix="pgt-release-suite-") as temp_name:
            temp = Path(temp_name)
            extension_dir = package_directory(artifact, temp)
            candidate_manifest = payload_manifest(extension_dir)
            attestation_path = args.result.with_suffix(".installation.json")
            attestation_path.write_text("[]\n", encoding="utf-8")
            environment = os.environ.copy()
            environment.update(
                {
                    "PGT_EXTENSION_DIR": str(extension_dir),
                    "PGT_RELEASE_CANDIDATE_COMMIT": args.candidate_commit,
                    "PGT_RELEASE_ARTIFACT_ID": artifact_spec["id"],
                    "PGT_RELEASE_ARTIFACT_SHA256": artifact_digest,
                    "PGT_RELEASE_PLATFORM": artifact_spec["platform"],
                    "PGT_RELEASE_BUILD_KIND": str(build_kind),
                    "PGT_RELEASE_FEATURE_SCOPE": str(feature_scope),
                    "PGT_RELEASE_ATTESTATION_PATH": str(attestation_path),
                    "PGT_RELEASE_MIN_POSTMASTER_START_EPOCH": str(attempt_started_at),
                    "PGT_RELEASE_MACHINE_FORMAT": "1",
                    "PGT_RELEASE_NEXTEST_RETRIES": "2",
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

            completed = subprocess.run(
                command,
                cwd=ROOT,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            output = completed.stdout.decode("utf-8", errors="replace")
            args.log.write_bytes(completed.stdout)
            sys.stdout.buffer.write(completed.stdout)
            machine_results = machine_summary(output)
            summaries = TEST_SUMMARY.findall(output)
            if machine_results is not None:
                passed, failed, skipped = machine_results
                executed = passed + failed
            elif summaries:
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

            version_match = POSTGRES_VERSION.search(output)
            postgres_version = version_match.group(1) if version_match else None
            attempt_case_results = machine_case_attempts(output, required_cases)
            observed_case_results = attempt_case_results[-1] if attempt_case_results else [
                {"id": identity, "status": "missing"} for identity in required_cases
            ]
            if required_cases and (
                not attempt_case_results
                or any(case["status"] != "passed" for attempt in attempt_case_results for case in attempt)
            ):
                status = "failed"
                print(
                    "ERROR: required case identities were not all observed: "
                    + ", ".join(
                        f"{case['id']}={case['status']}" for case in observed_case_results if case["status"] != "passed"
                    ),
                    file=sys.stderr,
                )
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
            if required_cases and (
                not attempt_case_results
                or any(case["status"] != "passed" for attempt in attempt_case_results for case in attempt)
            ):
                status = "failed"
            if completed.returncode == 0 and suite.get("requires_postgresql", False) and postgres_version is None:
                status = "failed"
                print("ERROR: suite did not report the actual PostgreSQL server version", file=sys.stderr)
            if completed.returncode == 0 and suite.get("kind") == "test" and executed == 0:
                status = "failed"
                print("ERROR: required test suite executed zero tests", file=sys.stderr)
            if completed.returncode == 0 and suite.get("kind") == "measurement" and measurement is None:
                status = "failed"
                print("ERROR: measurement suite did not write its result file", file=sys.stderr)

            if attestation_path.is_file():
                raw_attestations = json.loads(attestation_path.read_text(encoding="utf-8"))
                if not isinstance(raw_attestations, list):
                    raise ValueError("release installation attestation must contain a list")
                installation_observations = raw_attestations
            if suite.get("runtime") and not installation_observations:
                status = "failed"
                print("ERROR: runtime suite did not attest an installed candidate payload", file=sys.stderr)

        server_configuration = [
            observation.get("server", {}).get("settings", {})
            for observation in installation_observations
            if isinstance(observation.get("server"), dict)
        ]
        log_bytes = args.log.stat().st_size
        log = {
            "path": args.log.relative_to(ROOT).as_posix(),
            "bytes": log_bytes,
            "sha256": hashlib.sha256(args.log.read_bytes()).hexdigest(),
        }
        if not attempt_case_results:
            attempt_case_results = [[]]
        attempts = [
            {
                "attempt": index,
                "status": "passed" if all(case["status"] == "passed" for case in cases) else "failed",
                "exit_code": completed.returncode if index == len(attempt_case_results) else 1,
                "log": log,
                "cases": cases,
            }
            for index, cases in enumerate(attempt_case_results, start=1)
        ]
        retry_count = len(attempts) - 1
        record = {
            "suite_id": suite["id"],
            "candidate_commit": args.candidate_commit,
            "artifact_id": artifact_spec["id"],
            "artifact_digest": artifact_digest,
            "platform": artifact_spec["platform"],
            "candidate_identity": declared_identity,
            "build_kind": build_kind,
            "feature_scope": feature_scope,
            "postgresql_version": postgres_version,
            "suite_version": suite["suite_version"],
            "command": suite["command"],
            "command_argv": command,
            "effective_workload": suite["workload"],
            "executed_tests": executed,
            "skipped_tests": skipped,
            "selected_cases": required_cases,
            "observed_cases": observed_case_results,
            "shard": shard,
            "attempts": attempts,
            "retry_count": retry_count,
            "candidate_payload": candidate_manifest,
            "installation_observations": installation_observations,
            "server_configuration": server_configuration,
            "status": status,
            "log": log,
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
