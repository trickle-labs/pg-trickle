#!/usr/bin/env python3
"""Validate the v0.106.1 qualification contract and its fail-closed controls."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.106.1"
QUALIFICATION = ROOT / "tests/release/v0.106.1-qualification.json"
EXPECTED_SUITES = {
    "release-gate", "graph-v1", "delta-v1", "dvm-oracle", "recovery",
    "wal-admission", "pending-data-upgrade", "recreation", "logical-restore",
    "clone-isolation",
    "package-smoke", "upgrade-chain", "version-sync", "monitoring-contract",
    "criterion-regression", "database-workloads",
}
EXPECTED_ARTIFACTS = {
    "linux-amd64": "runtime-qualified",
    "linux-arm64": "build-only",
    "macos-arm64": "build-only",
    "windows-amd64": "best-effort",
}
CANDIDATE = "a" * 40


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"v0.106.1 release gate failed: {message}")


def job_block(workflow: str, name: str) -> str:
    match = re.search(rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)", workflow)
    require(match is not None, f"release workflow job {name!r} is missing")
    return match.group(1)


def needs(block: str, job: str) -> set[str]:
    match = re.search(r"(?m)^    needs:\s*(.+)$", block)
    require(match is not None, f"job {job!r} has no explicit needs dependency")
    return set(re.findall(r"[A-Za-z0-9_-]+", match.group(1)))


def synthetic_contract() -> dict[str, object]:
    return {
        "release_version": VERSION,
        "postgresql_major": 18,
        "source_versions": ["0.106.0"],
        "database_settings": {
            "fsync": "on", "synchronous_commit": "on", "full_page_writes": "on",
        },
        "evidence": {"schema_version": 3},
        "artifacts": [{
            "id": "linux-amd64", "platform": "linux-amd64",
            "support_tier": "build-only", "required": True,
        }],
        "required_suites": [
            {
                "id": "database-workloads", "kind": "measurement", "required": True,
                "artifact_id": "linux-amd64", "runtime": True, "upgrade": False,
                "requires_postgresql": True, "suite_version": VERSION,
                "command": "run smoke", "workload": "single smoke workload",
            },
            {
                "id": "criterion-regression", "kind": "measurement", "required": True,
                "artifact_id": "linux-amd64", "runtime": False, "upgrade": False,
                "requires_postgresql": False, "suite_version": VERSION,
                "command": "run Criterion", "workload": "small Criterion comparison",
            },
        ],
        "workloads": [{
            "id": "demo", "population_rows": 1, "batch_rows": 120,
            "offered_load": {
                "writer_concurrency": 1,
                "batch_interval_ms": 25,
                "refresh_interval_ms": 1000,
                "resource_sample_interval_ms": 250,
                "warmup_batches_per_repetition": 3,
                "measured_batches_per_repetition": 60,
                "repetitions": 3,
            },
            "required_cases": ["no-maintenance", "capture-only", "active-refresh"],
            "limits": {
                "source_write_p50_overhead_pct": 15,
                "refresh_p95_ms": 30000,
                "freshness_p95_ms": 60000,
                "cpu_percent_peak": 400,
                "memory_peak_bytes": 2147483648,
                "wal_bytes": 536870912,
                "change_backlog_rows": 100000,
                "temp_spill_bytes": 536870912,
                "output_log_bytes": 67108864,
                "storage_growth_bytes": 536870912,
            },
        }],
        "performance_budgets": [
            {"id": "criterion-regression", "metric": "maximum_mean_regression_pct", "threshold": 10, "minimum_absolute_delta_ns": 50, "unit": "percent"},
            {"id": "foreground-write-overhead", "metric": "source_write_p50_overhead_pct", "threshold": 15, "unit": "percent"},
        ],
    }


def suite_record(commit: str, digest: str, log_path: str, log_bytes: int, log_digest: str) -> dict[str, object]:
    return {
        "suite_id": "database-workloads", "candidate_commit": commit,
        "artifact_id": "linux-amd64", "artifact_digest": digest,
        "platform": "linux-amd64", "postgresql_version": "18.3",
        "suite_version": VERSION, "command": "run smoke",
        "effective_workload": "single smoke workload", "executed_tests": 1,
        "skipped_tests": 0, "status": "passed",
        "log": {"path": log_path, "bytes": log_bytes, "sha256": log_digest},
    }


def invoke_writer(case: str) -> tuple[int, str]:
    with tempfile.TemporaryDirectory(prefix=".v106-evidence-test-", dir=ROOT) as temp_name:
        temp = Path(temp_name)
        contract_path = temp / "qualification.json"
        contract = synthetic_contract()
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        artifact_path = temp / "pg_trickle-0.106.1-pg18-linux-amd64.tar.gz"
        artifact_path.write_bytes(b"test-package")
        artifact_digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        log_path = temp / "suite.log"
        log_path.write_bytes(b"passed\n" if case != "empty-log" else b"")
        log_digest = hashlib.sha256(log_path.read_bytes()).hexdigest()
        record = suite_record(
            "b" * 40 if case == "candidate-mismatch" else CANDIDATE,
            artifact_digest,
            log_path.relative_to(ROOT).as_posix(),
            log_path.stat().st_size,
            log_digest,
        )
        if case == "zero-tests":
            record["executed_tests"] = 0
        if case == "empty-log":
            record["log"]["bytes"] = 0
        result_path = temp / "suite.json"
        result_path.write_text(json.dumps(record), encoding="utf-8")

        measurement = {
            "kind": "database-workloads",
            "postgresql_version": "18.3",
            "durability_settings": {
                "fsync": "on", "synchronous_commit": "on", "full_page_writes": "on",
            },
            "candidate_commit": CANDIDATE,
            "artifact_digest": artifact_digest,
            "workloads": [{
                "id": "demo",
                "population_rows": 1,
                "batch_rows": 120,
                "offered_load": {
                    "writer_concurrency": 1,
                    "batch_interval_ms": 25,
                    "refresh_interval_ms": 1000,
                    "resource_sample_interval_ms": 250,
                    "warmup_batches_per_repetition": 3,
                    "measured_batches_per_repetition": 60,
                    "repetitions": 3,
                },
                "cases": {
                    "no-maintenance": {
                        "source_write_p50_ms": 8,
                        "source_write_p95_ms": 10,
                        "source_write_p99_ms": 12,
                        "throughput_rows_per_second": 1200,
                        "write_errors": 0,
                        "writer_concurrency": 1,
                        "batch_interval_ms": 25,
                        "refresh_interval_ms": 1000,
                        "resource_sample_interval_ms": 250,
                        "warmup_batches_per_repetition": 3,
                        "measured_batches_per_repetition": 60,
                        "repetitions": 3,
                        "exact_result": True,
                        "effective_strategy": "NONE",
                        "full_fallback_count": 0,
                    },
                    "capture-only": {
                        "source_write_p50_ms": 8,
                        "source_write_p95_ms": 10,
                        "source_write_p99_ms": 12,
                        "throughput_rows_per_second": 1000,
                        "write_errors": 0,
                        "writer_concurrency": 1,
                        "batch_interval_ms": 25,
                        "refresh_interval_ms": 1000,
                        "resource_sample_interval_ms": 250,
                        "warmup_batches_per_repetition": 3,
                        "measured_batches_per_repetition": 60,
                        "repetitions": 3,
                        "refresh_p95_ms": 1,
                        "freshness_p95_ms": 1,
                        "cpu_percent_peak": 1,
                        "memory_peak_bytes": 1,
                        "wal_bytes": 1,
                        "change_backlog_rows": 1,
                        "temp_spill_bytes": 0,
                        "output_log_bytes": 1,
                        "storage_growth_bytes": 1,
                        "exact_result": True,
                        "effective_strategy": "DIFFERENTIAL",
                        "full_fallback_count": 0,
                    },
                    "active-refresh": {
                        "source_write_p50_ms": 20 if case == "out-of-budget" else 8,
                        "source_write_p95_ms": 20 if case == "out-of-budget" else 10,
                        "source_write_p99_ms": 24 if case == "out-of-budget" else 12,
                        "throughput_rows_per_second": 1000,
                        "write_errors": 0,
                        "writer_concurrency": 1,
                        "batch_interval_ms": 25,
                        "refresh_interval_ms": 1000,
                        "resource_sample_interval_ms": 250,
                        "warmup_batches_per_repetition": 3,
                        "measured_batches_per_repetition": 60,
                        "repetitions": 3,
                        "refresh_p95_ms": 1,
                        "freshness_p95_ms": 1,
                        "cpu_percent_peak": 1,
                        "memory_peak_bytes": 1,
                        "wal_bytes": 1,
                        "change_backlog_rows": 1,
                        "temp_spill_bytes": 0,
                        "output_log_bytes": 1,
                        "storage_growth_bytes": 1,
                        "exact_result": True,
                        "effective_strategy": "DIFFERENTIAL",
                        "full_fallback_count": 0,
                    },
                },
            }],
        }
        record["measurement"] = measurement
        raw_measurement_path = temp / "measurement.json"
        raw_measurement_path.write_text(json.dumps(measurement), encoding="utf-8")
        raw_measurement = raw_measurement_path.read_bytes()
        record["measurement_evidence"] = {
            "path": raw_measurement_path.relative_to(ROOT).as_posix(),
            "bytes": len(raw_measurement),
            "sha256": hashlib.sha256(raw_measurement).hexdigest(),
            "attachments": [],
        }
        result_path.write_text(json.dumps(record), encoding="utf-8")
        criterion_measurement = {
            "kind": "criterion",
            "candidate_commit": CANDIDATE,
            "artifact_digest": artifact_digest,
            "baseline_version": "v0.106.0" if case == "criterion-baseline-mismatch" else "0.106.0",
            "compared_benchmarks": 113,
            "minimum_absolute_delta_ns": 49.0 if case == "criterion-floor-mismatch" else 50.0,
            "maximum_mean_regression_pct": 0.0,
            "maximum_raw_mean_regression_pct": 13.3,
            "subfloor_regressions": [{
                "label": "frontier_json/serialize/1",
                "baseline_ns": 121.873,
                "candidate_ns": 138.091,
                "delta_ns": 16.218,
                "regression_pct": 13.307,
            }],
        }
        criterion_measurement_path = temp / "criterion.json"
        criterion_measurement_path.write_text(json.dumps(criterion_measurement), encoding="utf-8")
        criterion_measurement_bytes = criterion_measurement_path.read_bytes()
        criterion_log = {
            "path": log_path.relative_to(ROOT).as_posix(),
            "bytes": log_path.stat().st_size,
            "sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
        }
        criterion_record = {
            "suite_id": "criterion-regression",
            "candidate_commit": CANDIDATE,
            "artifact_id": "linux-amd64",
            "artifact_digest": artifact_digest,
            "platform": "linux-amd64",
            "postgresql_version": None,
            "suite_version": VERSION,
            "command": "run Criterion",
            "effective_workload": "small Criterion comparison",
            "executed_tests": 113,
            "skipped_tests": 0,
            "status": "passed",
            "log": criterion_log,
            "measurement": criterion_measurement,
            "measurement_evidence": {
                "path": criterion_measurement_path.relative_to(ROOT).as_posix(),
                "bytes": len(criterion_measurement_bytes),
                "sha256": hashlib.sha256(criterion_measurement_bytes).hexdigest(),
                "attachments": [],
            },
        }
        criterion_result_path = temp / "criterion-suite.json"
        criterion_result_path.write_text(json.dumps(criterion_record), encoding="utf-8")
        command = [
            sys.executable, str(ROOT / "scripts/release_evidence.py"),
            "--output", str(temp / "evidence.json"), "--version", VERSION,
            "--candidate-commit", CANDIDATE, "--qualification", str(contract_path),
            "--suite-result", str(result_path), "--suite-result", str(criterion_result_path),
        ]
        if case != "missing-artifact":
            command.extend(["--artifact", str(artifact_path)])
        completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
        return completed.returncode, completed.stderr


def check_suite_runner() -> None:
    with tempfile.TemporaryDirectory(prefix=".v106-suite-runner-", dir=ROOT) as temp_name:
        temp = Path(temp_name)
        package = temp / "package"
        (package / "lib").mkdir(parents=True)
        (package / "extension").mkdir()
        (package / "lib/pg_trickle.so").write_bytes(b"test-library")
        (package / "extension/pg_trickle.control").write_text("default_version = '0.106.1'\n", encoding="utf-8")
        artifact = temp / "pg_trickle-0.106.1-pg18-linux-amd64.tar.gz"
        with tarfile.open(artifact, "w:gz") as archive:
            archive.add(package, arcname="pg_trickle-package")

        command = [sys.executable, "-c", "print('release suite runner smoke')"]
        contract = {
            "artifacts": [{"id": "linux-amd64", "platform": "linux-amd64"}],
            "required_suites": [{
                "id": "runner-smoke", "artifact_id": "linux-amd64", "kind": "check",
                "command": "runner smoke", "command_argv": command,
                "suite_version": VERSION, "workload": "temporary package runner smoke",
            }],
        }
        contract_path = temp / "qualification.json"
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        log = temp / "suite.log"
        result = temp / "suite.json"
        completed = subprocess.run([
            sys.executable, str(ROOT / "scripts/run_release_suite.py"),
            "--qualification", str(contract_path), "--suite", "runner-smoke",
            "--artifact", str(artifact), "--candidate-commit", CANDIDATE,
            "--log", log.relative_to(ROOT).as_posix(),
            "--result", result.relative_to(ROOT).as_posix(),
        ], cwd=ROOT, text=True, capture_output=True)
        require(completed.returncode == 0, f"release suite runner failed its smoke test: {completed.stderr}")
        record = json.loads(result.read_text(encoding="utf-8"))
        require(record.get("status") == "passed", "release suite runner did not record a pass")
        require(record.get("artifact_digest") == hashlib.sha256(artifact.read_bytes()).hexdigest(), "release suite runner artifact digest drift")
        require(record.get("log", {}).get("bytes", 0) > 0, "release suite runner did not retain its log")


def check_negative_controls() -> None:
    check_suite_runner()
    code, output = invoke_writer("valid")
    require(code == 0, f"valid structured evidence was rejected: {output}")
    for case, expected in (
        ("missing-artifact", "required package artifacts are missing"),
        ("zero-tests", "executed zero tests"),
        ("empty-log", "retained log is empty"),
        ("candidate-mismatch", "different candidate commit"),
        ("criterion-baseline-mismatch", "Criterion baseline does not match"),
        ("criterion-floor-mismatch", "Criterion materiality floor differs"),
        ("out-of-budget", "qualification is incomplete"),
    ):
        code, output = invoke_writer(case)
        require(code != 0 and expected in output, f"negative control {case!r} was not rejected")


def main() -> None:
    cargo = json.loads((ROOT / "META.json").read_text(encoding="utf-8"))
    require(cargo.get("version") == VERSION, "META.json version drift")
    contract = json.loads(QUALIFICATION.read_text(encoding="utf-8"))
    require(contract.get("release_version") == VERSION, "qualification version drift")
    require(contract.get("source_versions") == ["0.106.0"], "previous package boundary drift")
    require(contract.get("evidence", {}).get("schema_version") == 3, "structured evidence schema is not enabled")
    suites = contract.get("required_suites", [])
    ids = {suite.get("id") for suite in suites if isinstance(suite, dict)}
    require(ids == EXPECTED_SUITES and len(ids) == len(suites), "required suite set is incomplete or duplicated")
    require(all(suite.get("required") is True for suite in suites), "all listed qualification suites must be required")
    artifacts = contract.get("artifacts", [])
    require({item.get("id"): item.get("support_tier") for item in artifacts} == EXPECTED_ARTIFACTS, "artifact support tiers drifted")
    require(all(item.get("postgresql_major") == 18 and item.get("version") == VERSION for item in artifacts), "artifact target metadata drift")
    require(all(suite.get("artifact_id") == "linux-amd64" for suite in suites), "runtime suites must bind to Linux amd64 candidate package")
    require(any(suite.get("upgrade") for suite in suites), "previous-package upgrade suite is missing")
    require(any(suite.get("workload", "").startswith("logical database restore") for suite in suites), "logical restore suite is missing")
    require(any(suite.get("workload", "").startswith("database clone identity") for suite in suites), "clone-isolation suite is missing")
    require(all("offered_load" in workload for workload in contract.get("workloads", [])), "fixed workload dimensions are missing")
    criterion_budget = next(
        (item for item in contract.get("performance_budgets", []) if item.get("id") == "criterion-regression"),
        None,
    )
    require(criterion_budget is not None, "Criterion regression budget is missing")
    require(criterion_budget.get("minimum_absolute_delta_ns") == 50.0, "Criterion materiality floor must remain 50 ns")
    require(all(item.get("limits", {}).get("source_write_p50_overhead_pct") == 15.0 for item in contract.get("workloads", [])), "source-write p50 budget must remain 15%")

    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    preflight = job_block(workflow, "preflight")
    require("v0_106_1_release_gate.py" in preflight, "v0.106.1 release gate is not wired into preflight")
    qualification = job_block(workflow, "qualification")
    require("scripts/run_release_suite.py" in qualification, "qualification jobs do not emit structured suite results")
    require(
        "--suite-result" in workflow
        and re.search(r"--suite\s+[A-Za-z0-9_-]+=passed", workflow) is None,
        "release evidence still accepts manually asserted suite passes",
    )
    for job in ("publish-release", "publish-docker-arch"):
        block = job_block(workflow, job)
        require("qualification" in needs(block, job), f"{job} can publish without qualification")
    for relative, job in ((".github/workflows/ghcr.yml", "preflight"), (".github/workflows/pgxn.yml", "pgxn")):
        external = (ROOT / relative).read_text(encoding="utf-8")
        require('workflows: ["Release"]' in external and "workflow_run:" in external, f"{relative} does not wait for Release")
        require("workflow_run.conclusion == 'success'" in external, f"{relative} can promote failed qualification")
    require("bench_release_database_workloads" in (ROOT / "tests/e2e_bench_tests.rs").read_text(encoding="utf-8"), "live database workload suite is missing")
    require((ROOT / "scripts/qualify_live_package_upgrade.sh").is_file(), "real-package pending-data upgrade suite is missing")

    check_negative_controls()
    subprocess.run([sys.executable, str(ROOT / "scripts/generate_capability_manifest.py"), "--check"], cwd=ROOT, check=True)
    subprocess.run([str(ROOT / "scripts/check_version_sync.sh")], cwd=ROOT, check=True)
    print("v0.106.1 qualification, evidence, publication, and negative-control gates passed")


if __name__ == "__main__":
    main()
