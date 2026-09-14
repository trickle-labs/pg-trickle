#!/usr/bin/env python3
"""Write candidate-bound release evidence from structured suite results."""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path


RESULT_STATES = {"passed", "failed", "skipped", "unavailable", "stale", "historical"}


def parse_result(entry: str, *, default_status: str | None = None) -> dict[str, str]:
    name, separator, value = entry.partition("=")
    if not name or (not separator and default_status is None):
        raise ValueError(f"suite result must be NAME=STATUS: {entry!r}")
    status = default_status or value
    if status not in RESULT_STATES:
        raise ValueError(f"unknown suite status {status!r} for {name!r}")
    result = {"name": name, "status": status}
    if default_status is not None and separator and value:
        result["reason"] = value
    return result


def required_suite_ids(qualification: Path | None) -> list[str]:
    if qualification is None:
        return []
    contract = json.loads(qualification.read_text(encoding="utf-8"))
    return [suite["id"] for suite in contract.get("required_suites", []) if suite.get("required")]


def suite_specs(qualification: Path | None) -> dict[str, dict[str, str]]:
    if qualification is None:
        return {}
    contract = json.loads(qualification.read_text(encoding="utf-8"))
    return {
        suite["id"]: {
            key: suite[key]
            for key in ("command", "suite_version", "workload")
            if isinstance(suite.get(key), str)
        }
        for suite in contract.get("required_suites", [])
        if isinstance(suite, dict) and isinstance(suite.get("id"), str)
    }


def artifact_specs(qualification: Path | None) -> list[dict[str, object]]:
    if qualification is None:
        return []
    contract = json.loads(qualification.read_text(encoding="utf-8"))
    return [item for item in contract.get("artifacts", []) if isinstance(item, dict)]


def structured_evidence(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    contract = json.loads(args.qualification.read_text(encoding="utf-8"))
    if contract.get("release_version") != args.version:
        raise ValueError("qualification release version does not match evidence version")
    if contract.get("evidence", {}).get("schema_version") != 3:
        raise ValueError("structured suite results require evidence schema version 3")
    if not re.fullmatch(r"[0-9a-f]{40}", args.candidate_commit):
        raise ValueError("candidate commit must be a 40-character lowercase hexadecimal SHA")

    specs = {item["id"]: item for item in contract.get("required_suites", [])}
    artifacts: dict[str, dict[str, object]] = {}
    package_paths: list[str] = []
    for group in args.artifact:
        for value in group:
            matches = sorted(glob.glob(value)) if glob.has_magic(value) else [value]
            package_paths.extend(matches)
    for value in package_paths:
        path = Path(value)
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"release artifact {path} is missing or empty")
        matches = [item for item in contract.get("artifacts", []) if item["platform"] in path.name]
        if len(matches) != 1:
            raise ValueError(f"artifact {path.name!r} must match exactly one qualified platform")
        spec = matches[0]
        artifact_id = spec["id"]
        if artifact_id in artifacts:
            raise ValueError(f"duplicate package artifact for {artifact_id!r}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        record = {
            "artifact_id": artifact_id,
            "path": path.as_posix(),
            "bytes": path.stat().st_size,
            "sha256": digest,
            "platform": spec["platform"],
            "support_tier": spec["support_tier"],
            "build_status": "passed",
            "install_status": "not_run",
            "runtime_status": "not_run",
            "upgrade_status": "not_run",
            "reproducibility_status": "not_checked",
        }
        artifacts[artifact_id] = record

    missing_required = sorted(
        item["id"] for item in contract.get("artifacts", [])
        if item.get("required", True) and item["id"] not in artifacts
    )
    if missing_required:
        raise ValueError(f"required package artifacts are missing: {missing_required}")
    for item in contract.get("artifacts", []):
        if item["id"] not in artifacts:
            artifacts[item["id"]] = {
                "artifact_id": item["id"],
                "path": None,
                "bytes": 0,
                "sha256": None,
                "platform": item["platform"],
                "support_tier": item["support_tier"],
                "build_status": "unavailable",
                "install_status": "not_run",
                "runtime_status": "not_run",
                "upgrade_status": "not_run",
                "reproducibility_status": "not_checked",
            }

    suite_results: list[dict[str, object]] = []
    observed: set[str] = set()
    retained_logs: dict[str, dict[str, object]] = {}
    retained_measurements: list[dict[str, object]] = []
    runtime_outcomes: dict[str, list[str]] = {}
    upgrade_outcomes: dict[str, list[str]] = {}
    for result_path in args.suite_result:
        result = json.loads(result_path.read_text(encoding="utf-8"))
        suite_id = result.get("suite_id")
        spec = specs.get(suite_id)
        if spec is None or suite_id in observed:
            raise ValueError(f"unknown or duplicate suite result {suite_id!r}")
        observed.add(suite_id)
        if result.get("candidate_commit") != args.candidate_commit:
            raise ValueError(f"suite {suite_id!r} belongs to a different candidate commit")
        artifact = artifacts.get(result.get("artifact_id"))
        if artifact is None or artifact["build_status"] != "passed":
            raise ValueError(f"suite {suite_id!r} references an unavailable artifact")
        if result.get("artifact_digest") != artifact["sha256"]:
            raise ValueError(f"suite {suite_id!r} artifact digest does not match the published package")
        if result.get("artifact_id") != spec["artifact_id"] or result.get("platform") != artifact["platform"]:
            raise ValueError(f"suite {suite_id!r} ran against the wrong artifact or platform")
        for result_key, spec_key in (
            ("suite_version", "suite_version"),
            ("command", "command"),
            ("effective_workload", "workload"),
        ):
            if result.get(result_key) != spec.get(spec_key):
                raise ValueError(f"suite {suite_id!r} {result_key} does not match the contract")
        for count_key in ("executed_tests", "skipped_tests"):
            if not isinstance(result.get(count_key), int) or result[count_key] < 0:
                raise ValueError(f"suite {suite_id!r} has an invalid {count_key}")
        if result.get("status") not in RESULT_STATES:
            raise ValueError(f"suite {suite_id!r} has an invalid result state")
        if spec.get("kind") in {"test", "measurement"} and result["executed_tests"] == 0:
            raise ValueError(f"required suite {suite_id!r} executed zero tests")
        if spec.get("requires_postgresql"):
            version = result.get("postgresql_version")
            if not isinstance(version, str) or not version.startswith(f"{contract['postgresql_major']}."):
                raise ValueError(f"suite {suite_id!r} has no supported actual PostgreSQL version")

        log = result.get("log")
        if not isinstance(log, dict) or not isinstance(log.get("path"), str):
            raise ValueError(f"suite {suite_id!r} has no retained log record")
        log_path = (Path.cwd() / log["path"]).resolve()
        if not log_path.is_relative_to(Path.cwd().resolve()) or not log_path.is_file():
            raise ValueError(f"suite {suite_id!r} retained log is missing or outside the workspace")
        log_bytes = log_path.stat().st_size
        log_digest = hashlib.sha256(log_path.read_bytes()).hexdigest()
        if log_bytes == 0 or log.get("bytes") != log_bytes or log.get("sha256") != log_digest:
            raise ValueError(f"suite {suite_id!r} retained log is empty or has a mismatched digest")
        retained_logs[log["path"]] = {
            "name": suite_id,
            "path": log["path"],
            "bytes": log_bytes,
            "sha256": log_digest,
        }
        suite_results.append(result)
        if spec.get("runtime"):
            runtime_outcomes.setdefault(result["artifact_id"], []).append(result["status"])
        if spec.get("upgrade"):
            upgrade_outcomes.setdefault(result["artifact_id"], []).append(result["status"])
        if spec.get("kind") == "measurement":
            evidence = result.get("measurement_evidence")
            if not isinstance(evidence, dict) or not isinstance(evidence.get("path"), str):
                raise ValueError(f"suite {suite_id!r} has no retained raw measurement file")
            evidence_path = (Path.cwd() / evidence["path"]).resolve()
            if not evidence_path.is_relative_to(Path.cwd().resolve()) or not evidence_path.is_file():
                raise ValueError(f"suite {suite_id!r} raw measurement file is missing or outside the workspace")
            evidence_bytes = evidence_path.read_bytes()
            evidence_hash = hashlib.sha256(evidence_bytes).hexdigest()
            if not evidence_bytes or evidence.get("bytes") != len(evidence_bytes) or evidence.get("sha256") != evidence_hash:
                raise ValueError(f"suite {suite_id!r} raw measurement file is empty or has a mismatched digest")
            if json.loads(evidence_bytes) != result.get("measurement"):
                raise ValueError(f"suite {suite_id!r} raw measurement differs from its structured result")
            retained = {
                "suite_id": suite_id,
                "path": evidence["path"],
                "bytes": len(evidence_bytes),
                "sha256": evidence_hash,
                "attachments": [],
            }
            attachments = evidence.get("attachments", [])
            if not isinstance(attachments, list):
                raise ValueError(f"suite {suite_id!r} has invalid measurement attachments")
            for attachment in attachments:
                if not isinstance(attachment, dict) or not isinstance(attachment.get("path"), str):
                    raise ValueError(f"suite {suite_id!r} has an invalid raw measurement attachment")
                attachment_path = (Path.cwd() / attachment["path"]).resolve()
                if not attachment_path.is_relative_to(Path.cwd().resolve()) or not attachment_path.is_file():
                    raise ValueError(f"suite {suite_id!r} raw attachment is missing or outside the workspace")
                attachment_bytes = attachment_path.read_bytes()
                attachment_hash = hashlib.sha256(attachment_bytes).hexdigest()
                if not attachment_bytes or attachment.get("bytes") != len(attachment_bytes) or attachment.get("sha256") != attachment_hash:
                    raise ValueError(f"suite {suite_id!r} raw attachment is empty or has a mismatched digest")
                retained["attachments"].append({
                    "path": attachment["path"],
                    "bytes": len(attachment_bytes),
                    "sha256": attachment_hash,
                })
            retained_measurements.append(retained)

    for artifact_id, artifact in artifacts.items():
        runtime = runtime_outcomes.get(artifact_id, [])
        upgrades = upgrade_outcomes.get(artifact_id, [])
        if artifact["support_tier"] == "runtime-qualified":
            runtime_status = "passed" if runtime and all(item == "passed" for item in runtime) else "failed"
            upgrade_status = "passed" if upgrades and all(item == "passed" for item in upgrades) else "failed"
            artifact["install_status"] = runtime_status
            artifact["runtime_status"] = runtime_status
            artifact["upgrade_status"] = upgrade_status
        elif runtime:
            runtime_status = "passed" if all(item == "passed" for item in runtime) else "failed"
            artifact["install_status"] = runtime_status
            artifact["runtime_status"] = runtime_status

    required = {suite_id for suite_id, spec in specs.items() if spec.get("required", True)}
    missing_suites = sorted(required - observed)
    incomplete_suites = sorted(
        result["suite_id"] for result in suite_results
        if result["suite_id"] in required and result["status"] != "passed"
    )

    budget_results: list[dict[str, object]] = []
    measurements = [
        result["measurement"] for result in suite_results
        if isinstance(result.get("measurement"), dict)
    ]
    required_measurements = {
        suite_id for suite_id, spec in specs.items()
        if spec.get("required", True) and spec.get("kind") == "measurement"
    }
    observed_measurements = {
        result["suite_id"] for result in suite_results
        if isinstance(result.get("measurement"), dict)
    }
    if observed_measurements != required_measurements:
        raise ValueError(
            "required raw measurements are missing or unexpected: "
            f"missing={sorted(required_measurements - observed_measurements)}, "
            f"unexpected={sorted(observed_measurements - required_measurements)}"
        )
    for measurement in measurements:
        if measurement.get("candidate_commit") != args.candidate_commit:
            raise ValueError("measurement belongs to a different candidate commit")
        if measurement.get("artifact_digest") != artifacts["linux-amd64"]["sha256"]:
            raise ValueError("measurement artifact digest does not match the Linux amd64 package")
        if measurement.get("kind") == "criterion":
            spec = next(item for item in contract["performance_budgets"] if item["id"] == "criterion-regression")
            value = measurement.get("maximum_mean_regression_pct")
            raw_value = measurement.get("maximum_raw_mean_regression_pct")
            count = measurement.get("compared_benchmarks")
            minimum_delta_ns = measurement.get("minimum_absolute_delta_ns")
            subfloor_regressions = measurement.get("subfloor_regressions")
            if (
                not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not isinstance(raw_value, (int, float))
                or not math.isfinite(raw_value)
                or raw_value < value
                or not isinstance(count, int)
                or count < 1
                or not isinstance(minimum_delta_ns, (int, float))
                or not math.isfinite(minimum_delta_ns)
                or not isinstance(subfloor_regressions, list)
            ):
                raise ValueError("Criterion evidence must include compared benchmarks and maximum regression")
            if measurement.get("baseline_version") != contract.get("source_versions", [None])[0]:
                raise ValueError("Criterion baseline does not match the previous published version")
            if minimum_delta_ns != spec.get("minimum_absolute_delta_ns"):
                raise ValueError("Criterion materiality floor differs from the qualification contract")
            budget_results.append({
                "id": spec["id"],
                "metric": spec["metric"],
                "baseline": measurement.get("baseline_version"),
                "measured": value,
                "value": value,
                "threshold": spec["threshold"],
                "unit": spec["unit"],
                "compared_benchmarks": count,
                "maximum_raw_mean_regression_pct": raw_value,
                "minimum_absolute_delta_ns": minimum_delta_ns,
                "subfloor_regressions": subfloor_regressions,
                "verdict": "passed" if value <= spec["threshold"] else "failed",
            })
            continue
        if measurement.get("kind") != "database-workloads":
            raise ValueError("measurement suite has an unknown measurement kind")
        workload_suite = next(
            (result for result in suite_results if result.get("suite_id") == "database-workloads"),
            None,
        )
        if workload_suite is None:
            raise ValueError("database workload measurement has no matching suite result")
        if measurement.get("postgresql_version") != workload_suite.get("postgresql_version"):
            raise ValueError("database workload measurement PostgreSQL version differs from its suite record")
        if not str(measurement.get("postgresql_version", "")).startswith(f"{contract['postgresql_major']}."):
            raise ValueError("database workload measurement used an unsupported PostgreSQL major version")
        if measurement.get("durability_settings") != contract.get("database_settings"):
            raise ValueError("database workload durability settings differ from the frozen contract")
        observed_workloads = {item.get("id"): item for item in measurement.get("workloads", [])}
        expected_workloads = {item["id"]: item for item in contract.get("workloads", [])}
        if observed_workloads.keys() != expected_workloads.keys():
            raise ValueError("live workload set does not match the qualification contract")
        for workload_id, workload_spec in expected_workloads.items():
            workload = observed_workloads[workload_id]
            for key in ("population_rows", "batch_rows", "offered_load"):
                if workload.get(key) != workload_spec.get(key):
                    raise ValueError(f"workload {workload_id!r} {key} does not match the frozen contract")
            cases = workload.get("cases", {})
            required_cases = set(workload_spec["required_cases"])
            if cases.keys() != required_cases:
                raise ValueError(f"workload {workload_id!r} has missing or unexpected cases")
            baseline = cases.get("no-maintenance")
            if not isinstance(baseline, dict):
                raise ValueError(f"workload {workload_id!r} has no fixed source-write baseline")
            for case_id, case in cases.items():
                if not isinstance(case, dict):
                    raise ValueError(f"workload {workload_id!r} case {case_id!r} is invalid")
                for key, expected in workload_spec["offered_load"].items():
                    if case.get(key) != expected:
                        raise ValueError(f"workload {workload_id!r} case {case_id!r} {key} differs from contract")
                for metric in ("source_write_p50_ms", "source_write_p95_ms", "source_write_p99_ms", "throughput_rows_per_second", "write_errors"):
                    value = case.get(metric)
                    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                        raise ValueError(f"workload {workload_id!r} case {case_id!r} lacks {metric}")
                if case["throughput_rows_per_second"] <= 0 or case["write_errors"] != 0:
                    raise ValueError(f"workload {workload_id!r} case {case_id!r} has no successful source writes")
                if case_id != "no-maintenance" and case.get("exact_result") is not True:
                    raise ValueError(f"workload {workload_id!r} case {case_id!r} failed its exact-result check")
                expected_strategy = "NONE" if case_id == "no-maintenance" else "DIFFERENTIAL"
                if case.get("effective_strategy") != expected_strategy:
                    raise ValueError(f"workload {workload_id!r} case {case_id!r} did not use {expected_strategy}")
                if case_id != "no-maintenance" and case.get("full_fallback_count") != 0:
                    raise ValueError(f"workload {workload_id!r} case {case_id!r} used a full-refresh fallback")
            for case_id in required_cases - {"no-maintenance"}:
                case = cases[case_id]
                if not isinstance(case, dict):
                    raise ValueError(f"workload {workload_id!r} case {case_id!r} is invalid")
                for metric in ("source_write_p95_ms", "throughput_rows_per_second", "write_errors"):
                    value = case.get(metric)
                    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                        raise ValueError(f"workload {workload_id!r} case {case_id!r} lacks {metric}")
                baseline_ms = baseline.get("source_write_p95_ms")
                measured_ms = case["source_write_p95_ms"]
                if not isinstance(baseline_ms, (int, float)) or not math.isfinite(baseline_ms) or baseline_ms <= 0:
                    raise ValueError(f"workload {workload_id!r} baseline latency must be positive")
                overhead = 100 * (measured_ms / baseline_ms - 1)
                budget_results.append({
                    "id": f"{workload_id}.{case_id}.source-write-p95-overhead",
                    "metric": "source_write_p95_ms",
                    "baseline": baseline_ms,
                    "measured": measured_ms,
                    "value": overhead,
                    "threshold": workload_spec["limits"]["source_write_p95_overhead_pct"],
                    "unit": "percent",
                    "verdict": "passed" if overhead <= workload_spec["limits"]["source_write_p95_overhead_pct"] else "failed",
                })
                if case["write_errors"] != 0:
                    budget_results[-1]["verdict"] = "failed"
                for metric, threshold in workload_spec["limits"].items():
                    if metric == "source_write_p95_overhead_pct":
                        continue
                    value = case.get(metric)
                    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                        raise ValueError(f"workload {workload_id!r} case {case_id!r} lacks {metric}")
                    budget_results.append({
                        "id": f"{workload_id}.{case_id}.{metric}",
                        "metric": metric,
                        "baseline": None,
                        "measured": value,
                        "value": value,
                        "threshold": threshold,
                        "unit": "bytes" if metric.endswith("_bytes") else "milliseconds" if metric.endswith("_ms") else "rows" if metric.endswith("_rows") else "percent",
                        "verdict": "passed" if value <= threshold else "failed",
                    })

    failed_budgets = [item["id"] for item in budget_results if item["verdict"] != "passed"]
    global_write_budget = next(item for item in contract["performance_budgets"] if item["id"] == "foreground-write-overhead")
    write_overheads = [
        item["value"] for item in budget_results
        if item["id"].endswith("source-write-p95-overhead")
    ]
    if not write_overheads:
        raise ValueError("database workload evidence did not evaluate source-write overhead")
    maximum_write_overhead = max(write_overheads)
    budget_results.append({
        "id": global_write_budget["id"],
        "metric": global_write_budget["metric"],
        "baseline": "no-maintenance p95 per workload",
        "measured": maximum_write_overhead,
        "value": maximum_write_overhead,
        "threshold": global_write_budget["threshold"],
        "unit": global_write_budget["unit"],
        "verdict": "passed" if maximum_write_overhead <= global_write_budget["threshold"] else "failed",
    })
    if maximum_write_overhead > global_write_budget["threshold"]:
        failed_budgets.append(global_write_budget["id"])
    incomplete_artifacts = [
        artifact_id for artifact_id, artifact in artifacts.items()
        if artifact["support_tier"] == "runtime-qualified"
        and (artifact["runtime_status"] != "passed" or artifact["upgrade_status"] != "passed")
    ]
    status = "passed" if not missing_suites and not incomplete_suites and not failed_budgets and not incomplete_artifacts else "blocked"
    manifest = {
        "schema_version": 3,
        "release_version": args.version,
        "candidate_commit": args.candidate_commit,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "artifacts": sorted(artifacts.values(), key=lambda item: item["artifact_id"]),
        "support_matrix": [
            {key: item.get(key) for key in ("id", "platform", "support_tier", "required")}
            for item in contract.get("artifacts", [])
        ],
        "executed_suites": sorted(suite_results, key=lambda item: item["suite_id"]),
        "required_suites": sorted(required),
        "missing_required_suites": missing_suites,
        "incomplete_required_suites": incomplete_suites,
        "incomplete_runtime_qualified_artifacts": incomplete_artifacts,
        "performance_budgets": budget_results,
        "failed_performance_budgets": failed_budgets,
        "artifact_digest": sorted(item["sha256"] for item in artifacts.values() if item["sha256"]),
        "retained_logs": sorted(retained_logs.values(), key=lambda item: str(item["path"])),
        "retained_measurements": sorted(retained_measurements, key=lambda item: str(item["suite_id"])),
        "status": status,
    }
    if args.capability_manifest is not None:
        manifest_bytes = args.capability_manifest.read_bytes()
        capability = json.loads(manifest_bytes)
        if capability.get("release_version") != args.version:
            raise ValueError("capability manifest release version does not match evidence version")
        manifest["capability_manifest"] = {
            "version": capability["manifest_version"],
            "release_version": capability["release_version"],
            "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if status != "passed":
        parser.error(
            "qualification is incomplete: "
            + ", ".join(missing_suites + incomplete_suites + failed_budgets + incomplete_artifacts)
        )


def parse_logs(entries: list[str]) -> list[dict[str, str | int]]:
    logs: list[dict[str, str | int]] = []
    for entry in entries:
        name, separator, path_string = entry.partition("=")
        if not separator:
            path_string = name
            name = Path(path_string).stem
        path = Path(path_string)
        logs.append(
            {
                "name": name,
                "path": path.as_posix(),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return logs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--qualification", type=Path)
    parser.add_argument("--capability-manifest", type=Path)
    parser.add_argument("--postgresql-version")
    parser.add_argument("--suite-version")
    parser.add_argument("--workload")
    parser.add_argument("--artifact", nargs="+", action="append", default=[])
    parser.add_argument("--suite", action="append", default=[])
    parser.add_argument("--suite-result", action="append", type=Path, default=[])
    parser.add_argument("--skipped", action="append", default=[])
    parser.add_argument("--required-suite", action="append", default=[])
    parser.add_argument("--log", "--retained-log", dest="logs", action="append", default=[])
    args = parser.parse_args()

    try:
        if args.qualification is not None:
            qualification_contract = json.loads(args.qualification.read_text(encoding="utf-8"))
            if qualification_contract.get("evidence", {}).get("schema_version") == 3:
                structured_evidence(args, parser)
                return
        if not re.fullmatch(r"[0-9a-f]{40}", args.candidate_commit):
            raise ValueError("candidate commit must be a 40-character lowercase hexadecimal SHA")
        specs = suite_specs(args.qualification)
        platform_specs = artifact_specs(args.qualification)
        if args.qualification is not None:
            contract = json.loads(args.qualification.read_text(encoding="utf-8"))
            if contract.get("release_version") != args.version:
                raise ValueError("qualification release version does not match evidence version")
            if not args.postgresql_version or not args.suite_version or not args.workload:
                raise ValueError(
                    "qualification evidence requires PostgreSQL version, suite version, and workload"
                )

        artifacts = []
        observed_artifact_ids: set[str] = set()
        for path_string in (path for group in args.artifact for path in group):
            path = Path(path_string)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            artifact = {"path": path.as_posix(), "bytes": path.stat().st_size, "sha256": digest}
            matches = [item for item in platform_specs if item.get("platform") in path.name]
            if platform_specs and len(matches) != 1:
                raise ValueError(f"artifact {path.name!r} must match exactly one qualified platform")
            if matches:
                spec = matches[0]
                artifact_id = spec.get("id")
                if not isinstance(artifact_id, str) or artifact_id in observed_artifact_ids:
                    raise ValueError("qualified artifact identifiers must be unique")
                observed_artifact_ids.add(artifact_id)
                artifact.update(
                    {
                        "artifact_id": artifact_id,
                        "platform": spec.get("platform"),
                        "build_status": "passed",
                        "runtime_status": spec.get("runtime_status", "not_recorded"),
                        "runtime_suites": spec.get("runtime_suites", []),
                        "control_suites": spec.get("control_suites", []),
                    }
                )
            artifacts.append(artifact)

        expected_artifact_ids = {
            item["id"] for item in platform_specs if isinstance(item.get("id"), str)
        }
        if platform_specs and observed_artifact_ids != expected_artifact_ids:
            raise ValueError(
                "release artifacts do not match the qualification contract: "
                f"missing={sorted(expected_artifact_ids - observed_artifact_ids)}, "
                f"unexpected={sorted(observed_artifact_ids - expected_artifact_ids)}"
            )

        results = [parse_result(entry) for entry in args.suite]
        results.extend(parse_result(entry, default_status="skipped") for entry in args.skipped)
        names = [result["name"] for result in results]
        if len(names) != len(set(names)):
            raise ValueError("duplicate suite result identifier")
        for result in results:
            spec = specs.get(result["name"])
            if spec is not None:
                result.update(spec)

        required = set(args.required_suite or required_suite_ids(args.qualification))
        actual = {result["name"]: result for result in results}
        missing = sorted(required - actual.keys())
        incomplete = sorted(
            name for name in required if name in actual and actual[name]["status"] != "passed"
        )
        logs = parse_logs(args.logs)
        if args.qualification is not None and (not artifacts or not logs):
            raise ValueError("qualification evidence requires at least one artifact and retained log")
        status = "passed" if not missing and not incomplete else "blocked"

        capability_manifest = None
        if args.capability_manifest is not None:
            manifest = json.loads(args.capability_manifest.read_text(encoding="utf-8"))
            manifest_version = manifest.get("manifest_version")
            manifest_release = manifest.get("release_version")
            if not isinstance(manifest_version, int) or not isinstance(manifest_release, str):
                raise ValueError("capability manifest is missing version metadata")
            if manifest_release != args.version:
                raise ValueError("capability manifest release version does not match evidence version")
            capability_manifest = {
                "version": manifest_version,
                "release_version": manifest_release,
                "sha256": hashlib.sha256(args.capability_manifest.read_bytes()).hexdigest(),
            }

        evidence = {
            "schema_version": 2,
            "release_version": args.version,
            "candidate_commit": args.candidate_commit,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "artifacts": sorted(artifacts, key=lambda item: item["path"]),
            "executed_suites": sorted(results, key=lambda item: item["name"]),
            "required_suites": sorted(required),
            "missing_required_suites": missing,
            "incomplete_required_suites": incomplete,
            "artifact_digest": sorted(item["sha256"] for item in artifacts),
            "postgresql_version": args.postgresql_version,
            "suite_version": args.suite_version,
            "workload": args.workload,
            "retained_logs": sorted(logs, key=lambda item: str(item["path"])),
            "status": status,
        }
        if capability_manifest is not None:
            evidence["capability_manifest"] = capability_manifest
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
        if status != "passed":
            raise ValueError(
                "required release suites are incomplete: "
                + ", ".join(missing + incomplete)
            )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
