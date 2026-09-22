#!/usr/bin/env python3
"""Write candidate-bound release evidence from structured suite results."""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import math
import re
import tarfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

if __package__:
    from .run_release_suite import machine_case_attempts
else:
    from run_release_suite import machine_case_attempts


RESULT_STATES = {"passed", "failed", "skipped", "unavailable", "stale", "historical"}


def canonical_digest(value: object) -> str:
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def archive_identity(path: Path) -> dict[str, object] | None:
    try:
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path) as archive:
                names = [name for name in archive.namelist() if name.endswith("/release-candidate.json")]
                if len(names) != 1:
                    return None
                return json.loads(archive.read(names[0]))
        with tarfile.open(path, "r:gz") as archive:
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
        raise ValueError(f"could not read candidate identity from {path}: {error}") from error


def archive_payload_manifest(path: Path) -> dict[str, object]:
    entries: list[tuple[str, bytes]] = []
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.endswith("/release-candidate.json")]
            if len(names) != 1:
                raise ValueError(f"release artifact {path.name!r} has no unique candidate root")
            root = names[0].rsplit("/", 1)[0]
            for member in archive.infolist():
                if not member.is_dir():
                    entries.append((member.filename, archive.read(member)))
    else:
        with tarfile.open(path, "r:gz") as archive:
            members = archive.getmembers()
            identity_members = [member for member in members if member.name.endswith("/release-candidate.json")]
            if len(identity_members) != 1:
                raise ValueError(f"release artifact {path.name!r} has no unique candidate root")
            root = identity_members[0].name.rsplit("/", 1)[0]
            for member in members:
                if not member.isfile():
                    continue
                stream = archive.extractfile(member)
                if stream is not None:
                    entries.append((member.name, stream.read()))

    files: dict[str, dict[str, int | str]] = {}
    for name, data in entries:
        for source_root, destination_root in (
            (f"{root}/lib/", "usr/lib/postgresql/18/lib/"),
            (f"{root}/extension/", "usr/share/postgresql/18/extension/"),
        ):
            if name.startswith(source_root):
                relative = name[len(source_root):]
                destination = destination_root + relative
                if destination in files:
                    raise ValueError(f"release artifact {path.name!r} has duplicate payload file {destination!r}")
                files[destination] = {
                    "path": destination,
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
                break
    ordered = [files[name] for name in sorted(files)]
    if not ordered:
        raise ValueError(f"release artifact {path.name!r} contains no candidate payload")
    return {"files": ordered, "sha256": canonical_digest(ordered)}


def validate_case_evidence(result: dict[str, object], spec: dict[str, object]) -> None:
    required = spec.get("required_cases", [])
    if not required:
        return
    selected = result.get("selected_cases")
    if selected != required:
        raise ValueError(f"suite {result.get('suite_id')!r} selected cases do not match the contract")
    observed = result.get("observed_cases")
    if not isinstance(observed, list):
        raise ValueError(f"suite {result.get('suite_id')!r} has no observed case outcomes")
    observed_ids: list[str] = []
    for case in observed:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str):
            raise ValueError(f"suite {result.get('suite_id')!r} has an invalid observed case")
        observed_ids.append(case["id"])
        if case.get("status") != "passed":
            raise ValueError(f"required case {case['id']!r} did not pass")
    if len(observed_ids) != len(set(observed_ids)) or set(observed_ids) != set(required):
        raise ValueError(f"suite {result.get('suite_id')!r} has missing or unexpected observed cases")


def validate_case_log(result: dict[str, object], spec: dict[str, object], output: str) -> None:
    required = spec.get("required_cases", [])
    if not required:
        return
    observed_attempts = machine_case_attempts(output, required)
    if not observed_attempts or [item.get("cases") for item in result["attempts"]] != observed_attempts:
        raise ValueError(f"suite {result.get('suite_id')!r} case attempts differ from machine events in its log")
    if result["observed_cases"] != observed_attempts[-1]:
        raise ValueError(f"suite {result.get('suite_id')!r} observed cases differ from machine events in its log")


def validate_attempts(result: dict[str, object], spec: dict[str, object]) -> None:
    attempts = result.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise ValueError(f"suite {result.get('suite_id')!r} has no retained attempts")
    indexes: list[int] = []
    for attempt in attempts:
        if not isinstance(attempt, dict) or not isinstance(attempt.get("attempt"), int):
            raise ValueError(f"suite {result.get('suite_id')!r} has an invalid attempt record")
        indexes.append(attempt["attempt"])
        if attempt.get("status") != "passed":
            raise ValueError(
                f"suite {result.get('suite_id')!r} retained a non-passing attempt; retries cannot erase it"
        )
        cases = attempt.get("cases")
        required_cases = spec.get("required_cases", [])
        if not isinstance(cases, list):
            raise ValueError(f"suite {result.get('suite_id')!r} retained a failed or missing case attempt")
        if required_cases and {case.get("id") for case in cases if isinstance(case, dict)} != set(required_cases):
            raise ValueError(f"suite {result.get('suite_id')!r} retained a failed or missing case attempt")
        if any(not isinstance(case, dict) or case.get("status") != "passed" for case in cases):
            raise ValueError(f"suite {result.get('suite_id')!r} retained a failed or missing case attempt")
        log = attempt.get("log")
        if not isinstance(log, dict) or not all(
            isinstance(log.get(key), expected)
            for key, expected in (("path", str), ("bytes", int), ("sha256", str))
        ):
            raise ValueError(f"suite {result.get('suite_id')!r} retained an invalid attempt log")
        log_path = (Path.cwd() / log["path"]).resolve()
        if not log_path.is_relative_to(Path.cwd().resolve()) or not log_path.is_file():
            raise ValueError(f"suite {result.get('suite_id')!r} retained attempt log is missing")
        if log["bytes"] <= 0 or log["bytes"] != log_path.stat().st_size:
            raise ValueError(f"suite {result.get('suite_id')!r} retained attempt log has the wrong size")
        if log["sha256"] != hashlib.sha256(log_path.read_bytes()).hexdigest():
            raise ValueError(f"suite {result.get('suite_id')!r} retained attempt log has the wrong digest")
    if indexes != list(range(1, len(indexes) + 1)):
        raise ValueError(f"suite {result.get('suite_id')!r} has non-contiguous attempt numbers")
    if result.get("retry_count") != len(attempts) - 1:
        raise ValueError(f"suite {result.get('suite_id')!r} retry_count does not match attempts")
    if spec.get("required", True) and result.get("status") != "passed":
        raise ValueError(f"required suite {result.get('suite_id')!r} did not pass all attempts")


def validate_installation(
    result: dict[str, object],
    spec: dict[str, object],
    artifact: dict[str, object],
    database_settings: dict[str, str],
) -> None:
    if not spec.get("runtime"):
        return
    observations = result.get("installation_observations")
    if not isinstance(observations, list) or not observations:
        raise ValueError(f"runtime suite {result.get('suite_id')!r} has no installed candidate observation")
    candidate_payload = result.get("candidate_payload")
    if not isinstance(candidate_payload, dict) or not isinstance(candidate_payload.get("files"), list):
        raise ValueError(f"runtime suite {result.get('suite_id')!r} has no candidate payload manifest")
    files = candidate_payload["files"]
    if candidate_payload.get("sha256") != canonical_digest(files):
        raise ValueError(f"runtime suite {result.get('suite_id')!r} candidate payload digest is invalid")
    if artifact.get("payload_manifest") is not None and candidate_payload != artifact["payload_manifest"]:
        raise ValueError(f"runtime suite {result.get('suite_id')!r} payload is not the declared archive payload")
    server_configuration = result.get("server_configuration")
    expected_server_configuration = [
        observation.get("server", {}).get("settings", {})
        for observation in observations
        if isinstance(observation, dict) and isinstance(observation.get("server"), dict)
    ]
    if server_configuration != expected_server_configuration:
        raise ValueError(f"runtime suite {result.get('suite_id')!r} server configuration evidence is inconsistent")
    for observation in observations:
        if not isinstance(observation, dict):
            raise ValueError(f"runtime suite {result.get('suite_id')!r} has an invalid installation observation")
        for key, expected in (
            ("candidate_commit", result.get("candidate_commit")),
            ("artifact_id", result.get("artifact_id")),
            ("artifact_digest", result.get("artifact_digest")),
            ("platform", result.get("platform")),
            ("build_kind", result.get("build_kind")),
            ("feature_scope", result.get("feature_scope")),
        ):
            if observation.get(key) != expected:
                raise ValueError(f"runtime suite {result.get('suite_id')!r} installed payload has the wrong {key}")
        if observation.get("artifact_digest") != artifact.get("sha256"):
            raise ValueError(f"runtime suite {result.get('suite_id')!r} installed payload is not the published artifact")
        if observation.get("payload_digest") != candidate_payload["sha256"]:
            raise ValueError(f"runtime suite {result.get('suite_id')!r} candidate payload digest differs from installation")
        if observation.get("installed_payload_digest") != candidate_payload["sha256"]:
            raise ValueError(f"runtime suite {result.get('suite_id')!r} installed payload digest differs from candidate")
        if observation.get("candidate_files") != files or observation.get("installed_files") != files:
            raise ValueError(f"runtime suite {result.get('suite_id')!r} installed files differ from candidate")
        server = observation.get("server")
        if not isinstance(server, dict):
            raise ValueError(f"runtime suite {result.get('suite_id')!r} has no server identity")
        container_id = server.get("container_id")
        if not isinstance(container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise ValueError(f"runtime suite {result.get('suite_id')!r} has an invalid container identity")
        try:
            start_epoch = float(server["postmaster_start_epoch"])
            minimum_epoch = float(server["minimum_postmaster_start_epoch"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"runtime suite {result.get('suite_id')!r} has incomplete postmaster identity") from error
        if not math.isfinite(start_epoch) or not math.isfinite(minimum_epoch) or start_epoch <= 0:
            raise ValueError(f"runtime suite {result.get('suite_id')!r} has invalid postmaster timestamps")
        if server.get("fresh_postmaster") is not (start_epoch >= minimum_epoch):
            raise ValueError(f"runtime suite {result.get('suite_id')!r} did not use a fresh PostgreSQL process")
        if not str(server.get("server_version", "")).startswith("18."):
            raise ValueError(f"runtime suite {result.get('suite_id')!r} used an unsupported PostgreSQL server")
        settings = server.get("settings")
        if not isinstance(settings, dict) or any(
            settings.get(key) != expected for key, expected in database_settings.items()
        ):
            raise ValueError(f"runtime suite {result.get('suite_id')!r} server durability settings drifted")


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
    required_fields = contract.get("evidence", {}).get("required_suite_fields", [])
    if not isinstance(required_fields, list) or not all(isinstance(field, str) for field in required_fields):
        raise ValueError("qualification evidence has invalid required suite fields")
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
        identity = archive_identity(path)
        if contract.get("candidate_identity", {}).get("required"):
            if not isinstance(identity, dict):
                raise ValueError(f"release artifact {path.name!r} has no candidate identity")
            for key, expected in (
                ("candidate_commit", args.candidate_commit),
                ("build_kind", contract.get("build_kind", "exact-release")),
                ("feature_scope", contract.get("feature_scope", "default-release")),
            ):
                if identity.get(key) != expected:
                    raise ValueError(f"release artifact {path.name!r} has the wrong candidate {key}")
            if identity.get("build_provenance") != contract.get("build_provenance"):
                raise ValueError(f"release artifact {path.name!r} has untrusted build provenance")
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
            "candidate_identity": identity,
        }
        if contract.get("candidate_identity", {}).get("required"):
            record["payload_manifest"] = archive_payload_manifest(path)
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
        missing_fields = sorted(field for field in required_fields if field not in result)
        if missing_fields:
            raise ValueError(f"suite {suite_id!r} is missing required evidence fields: {missing_fields}")
        if result.get("candidate_commit") != args.candidate_commit:
            raise ValueError(f"suite {suite_id!r} belongs to a different candidate commit")
        artifact = artifacts.get(result.get("artifact_id"))
        if artifact is None or artifact["build_status"] != "passed":
            raise ValueError(f"suite {suite_id!r} references an unavailable artifact")
        if result.get("artifact_digest") != artifact["sha256"]:
            raise ValueError(f"suite {suite_id!r} artifact digest does not match the published package")
        if result.get("artifact_id") != spec["artifact_id"] or result.get("platform") != artifact["platform"]:
            raise ValueError(f"suite {suite_id!r} ran against the wrong artifact or platform")
        if "build_kind" in required_fields and result.get("build_kind") != contract.get("build_kind", "exact-release"):
            raise ValueError(f"suite {suite_id!r} has the wrong build kind")
        if "feature_scope" in required_fields and result.get("feature_scope") != contract.get("feature_scope", "default-release"):
            raise ValueError(f"suite {suite_id!r} has the wrong feature scope")
        identity = result.get("candidate_identity")
        artifact_identity_value = artifact.get("candidate_identity")
        if contract.get("candidate_identity", {}).get("required"):
            if not isinstance(identity, dict) or identity != artifact_identity_value:
                raise ValueError(f"suite {suite_id!r} candidate identity does not match the artifact")
        for result_key, spec_key in (
            ("suite_version", "suite_version"),
            ("command", "command"),
            ("effective_workload", "workload"),
        ):
            if result.get(result_key) != spec.get(spec_key):
                raise ValueError(f"suite {suite_id!r} {result_key} does not match the contract")
        if "command_argv" in required_fields and result.get("command_argv") != spec.get("command_argv"):
            raise ValueError(f"suite {suite_id!r} command argv does not match the contract")
        for count_key in ("executed_tests", "skipped_tests"):
            if not isinstance(result.get(count_key), int) or result[count_key] < 0:
                raise ValueError(f"suite {suite_id!r} has an invalid {count_key}")
        if result["skipped_tests"] > spec.get("allowed_skipped_tests", 0):
            raise ValueError(f"suite {suite_id!r} reported unexpected skipped tests")
        if result.get("status") not in RESULT_STATES:
            raise ValueError(f"suite {suite_id!r} has an invalid result state")
        if spec.get("kind") in {"test", "measurement"} and result["executed_tests"] == 0:
            raise ValueError(f"required suite {suite_id!r} executed zero tests")
        if spec.get("requires_postgresql"):
            version = result.get("postgresql_version")
            if not isinstance(version, str) or not version.startswith(f"{contract['postgresql_major']}."):
                raise ValueError(f"suite {suite_id!r} has no supported actual PostgreSQL version")

        if "selected_cases" in required_fields:
            if not isinstance(result.get("selected_cases"), list) or not result["selected_cases"] and spec.get("required_cases"):
                raise ValueError(f"suite {suite_id!r} has an empty case selection")
            validate_case_evidence(result, spec)
        if "attempts" in required_fields:
            validate_attempts(result, spec)
        if "installation_observations" in required_fields:
            validate_installation(result, spec, artifact, contract.get("database_settings", {}))

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
        validate_case_log(result, spec, log_path.read_text(encoding="utf-8", errors="replace"))
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

    required_shards = contract.get("required_shards", [])
    if required_shards:
        expected_shards = {item["id"]: item for item in required_shards}
        if len(expected_shards) != len(required_shards):
            raise ValueError("qualification contract has duplicate required shard identifiers")
        shard_suite_ids = {item["suite_id"] for item in required_shards}
        observed_shards: dict[str, dict[str, object]] = {}
        assigned_cases: dict[str, str] = {}
        for result in suite_results:
            if result["suite_id"] not in shard_suite_ids:
                continue
            shard = result.get("shard")
            if not isinstance(shard, dict) or not isinstance(shard.get("id"), str):
                raise ValueError(f"suite {result['suite_id']!r} has no valid shard identity")
            shard_id = shard["id"]
            expected = expected_shards.get(shard_id)
            if expected is None or shard_id in observed_shards:
                raise ValueError(f"unknown or duplicate qualification shard {shard_id!r}")
            if shard.get("suite_id", result["suite_id"]) != expected["suite_id"]:
                raise ValueError(f"qualification shard {shard_id!r} is bound to the wrong suite")
            if shard.get("index") != expected["index"] or shard.get("count") != expected["count"]:
                raise ValueError(f"qualification shard {shard_id!r} has the wrong position")
            required_cases = expected.get("required_cases", [])
            if result.get("selected_cases") != required_cases:
                raise ValueError(f"qualification shard {shard_id!r} selected the wrong cases")
            for case in result.get("observed_cases", []):
                case_id = case["id"]
                if case_id in assigned_cases:
                    raise ValueError(f"required case {case_id!r} was assigned to multiple shards")
                assigned_cases[case_id] = shard_id
            observed_shards[shard_id] = result
        missing_shards = sorted(set(expected_shards) - set(observed_shards))
        if missing_shards:
            raise ValueError(f"required qualification shards are missing: {missing_shards}")
        required_cases = {
            case_id
            for shard in required_shards
            for case_id in shard.get("required_cases", [])
        }
        if set(contract.get("required_cases", [])) != required_cases:
            raise ValueError("qualification contract required_cases do not match shard coverage")
        if set(assigned_cases) != required_cases:
            raise ValueError(
                "required qualification cases have incomplete shard coverage: "
                f"missing={sorted(required_cases - set(assigned_cases))}, "
                f"unexpected={sorted(set(assigned_cases) - required_cases)}"
            )

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
            no_maintenance = cases.get("no-maintenance")
            if not isinstance(no_maintenance, dict):
                raise ValueError(f"workload {workload_id!r} has no fixed source-write baseline")
            capture_only = cases.get("capture-only")
            if not isinstance(capture_only, dict):
                raise ValueError(f"workload {workload_id!r} has no capture-only source-write baseline")
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
            for case_id in required_cases - {"no-maintenance", "capture-only"}:
                case = cases[case_id]
                if not isinstance(case, dict):
                    raise ValueError(f"workload {workload_id!r} case {case_id!r} is invalid")
                for metric in ("source_write_p50_ms", "throughput_rows_per_second", "write_errors"):
                    value = case.get(metric)
                    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                        raise ValueError(f"workload {workload_id!r} case {case_id!r} lacks {metric}")
                baseline_ms = capture_only.get("source_write_p50_ms")
                measured_ms = case["source_write_p50_ms"]
                if not isinstance(baseline_ms, (int, float)) or not math.isfinite(baseline_ms) or baseline_ms <= 0:
                    raise ValueError(f"workload {workload_id!r} baseline latency must be positive")
                overhead = 100 * (measured_ms / baseline_ms - 1)
                budget_results.append({
                    "id": f"{workload_id}.{case_id}.source-write-p50-overhead",
                    "metric": "source_write_p50_ms",
                    "baseline": baseline_ms,
                    "measured": measured_ms,
                    "value": overhead,
                    "threshold": workload_spec["limits"]["source_write_p50_overhead_pct"],
                    "unit": "percent",
                    "verdict": "passed" if overhead <= workload_spec["limits"]["source_write_p50_overhead_pct"] else "failed",
                })
                if case["write_errors"] != 0:
                    budget_results[-1]["verdict"] = "failed"
                for metric, threshold in workload_spec["limits"].items():
                    if metric == "source_write_p50_overhead_pct":
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
        if item["id"].endswith("source-write-p50-overhead")
    ]
    if not write_overheads:
        raise ValueError("database workload evidence did not evaluate source-write overhead")
    maximum_write_overhead = max(write_overheads)
    budget_results.append({
        "id": global_write_budget["id"],
        "metric": global_write_budget["metric"],
        "baseline": "capture-only p50 per workload",
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
        "required_cases": sorted(
            {
                case_id
                for shard in contract.get("required_shards", [])
                for case_id in shard.get("required_cases", [])
            }
        ),
        "required_shards": sorted(
            [item["id"] for item in contract.get("required_shards", [])]
        ),
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
