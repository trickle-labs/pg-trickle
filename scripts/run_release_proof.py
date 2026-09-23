#!/usr/bin/env python3
"""Exercise the release runner and evidence writer on a packaged PR candidate."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QUALIFICATION = ROOT / "tests/release/v0.108.0-qualification.json"
SUITES = ("sensitivity-baseline", "recovery", "upgrade-chain", "recreation")


def archive_package(package: Path, archive: Path) -> None:
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(package, arcname=package.name)


def run_writer(
    contract: Path, artifact: Path, results: list[Path], output: Path, commit: str
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable, "scripts/release_evidence.py", "--output", str(output),
        "--version", "0.108.0", "--candidate-commit", commit,
        "--qualification", str(contract), "--artifact", str(artifact),
    ]
    for result in results:
        command.extend(("--suite-result", str(result)))
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def verify_retained_evidence(output: Path, evidence_path: Path) -> None:
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    references = list(evidence.get("retained_logs", []))
    measurements = evidence.get("retained_measurements", [])
    references.extend(measurements)
    for measurement in measurements:
        references.extend(measurement.get("attachments", []))
    for reference in references:
        path = (ROOT / reference["path"]).resolve()
        if not path.is_relative_to(output.resolve()) or not path.is_file():
            raise RuntimeError(f"retained evidence is missing from the uploaded proof bundle: {reference}")
        data = path.read_bytes()
        if (
            len(data) != reference.get("bytes")
            or hashlib.sha256(data).hexdigest() != reference.get("sha256")
        ):
            raise RuntimeError(f"retained evidence changed before upload: {reference['path']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-dir", type=Path, required=True)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--platform", default="linux-amd64")
    parser.add_argument("--workflow", default="ci.yml")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "target/release-proof")
    args = parser.parse_args()

    if not re.fullmatch(r"[0-9a-f]{40}", args.candidate_commit):
        parser.error("candidate commit must be a lowercase 40-character SHA")
    source = args.package_dir.resolve()
    for relative in (
        "usr/lib/postgresql/18/lib/pg_trickle.so",
        "usr/share/postgresql/18/extension/pg_trickle.control",
    ):
        if not (source / relative).is_file():
            parser.error(f"package is missing {relative}")

    output = args.output_dir.resolve()
    if output == ROOT / "target" or not output.is_relative_to(ROOT / "target"):
        parser.error("output directory must be a subdirectory of target/")
    if source.is_relative_to(output):
        parser.error("package directory must be outside the output directory")
    shutil.rmtree(output, ignore_errors=True)
    output.mkdir(parents=True)
    package = output / f"pg_trickle-0.108.0-pg18-{args.platform}"
    shutil.copytree(source / "usr/lib/postgresql/18/lib", package / "lib")
    shutil.copytree(source / "usr/share/postgresql/18/extension", package / "extension")

    contract = json.loads(QUALIFICATION.read_text(encoding="utf-8"))
    contract["build_kind"] = "pr-package"
    contract["build_provenance"] = {
        "workflow": args.workflow, "cargo_profile": "release", "instrumentation": "none"
    }
    contract["required_suites"] = [
        suite for suite in contract["required_suites"] if suite["id"] in SUITES
    ]
    for suite in contract["required_suites"]:
        suite["artifact_id"] = args.platform
    contract["artifacts"] = [
        artifact for artifact in contract["artifacts"] if artifact["id"] == args.platform
    ]
    if len(contract["artifacts"]) != 1:
        parser.error(f"platform {args.platform!r} is not in the release contract")
    docker_platform = args.platform.replace("-", "/", 1)
    contract["artifacts"][0]["support_tier"] = "runtime-qualified"
    contract["performance_budgets"] = []
    contract["workloads"] = []
    contract_path = output / "qualification.json"
    contract_path.write_text(json.dumps(contract, indent=2) + "\n", encoding="utf-8")
    identity = {
        "candidate_commit": args.candidate_commit,
        "build_kind": contract["build_kind"],
        "feature_scope": contract["feature_scope"],
        "build_provenance": contract["build_provenance"],
    }
    (package / "release-candidate.json").write_text(
        json.dumps(identity) + "\n", encoding="utf-8"
    )
    artifact = output / f"{package.name}.tar.gz"
    archive_package(package, artifact)

    context = output / "context/candidate"
    shutil.copytree(package / "lib", context / "lib")
    shutil.copytree(package / "extension", context / "extension")
    subprocess.run(
        [
            "docker", "buildx", "build", "--load", "--provenance=false",
            "--platform", docker_platform,
            "-t", "pg_trickle_release_candidate:local",
            "-f", "tests/Dockerfile.release-e2e", str(context.parent),
        ],
        cwd=ROOT,
        check=True,
    )

    environment = os.environ.copy()
    environment["DOCKER_DEFAULT_PLATFORM"] = docker_platform
    environment["PGS_E2E_PLATFORM"] = docker_platform
    results: list[Path] = []
    for suite in SUITES:
        result = output / f"{suite}.json"
        results.append(result)
        subprocess.run(
            [
                sys.executable, "scripts/run_release_suite.py",
                "--qualification", str(contract_path), "--suite", suite,
                "--artifact", str(artifact), "--candidate-commit", args.candidate_commit,
                "--log", str(output / f"{suite}.log"), "--result", str(result),
            ],
            cwd=ROOT,
            env=environment,
            check=True,
        )
    evidence = output / "RELEASE-EVIDENCE.json"
    completed = run_writer(contract_path, artifact, results, evidence, args.candidate_commit)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr)
    if json.loads(evidence.read_text(encoding="utf-8"))["status"] != "passed":
        raise RuntimeError("runner-produced evidence did not pass")
    verify_retained_evidence(output, evidence)

    negative = output / "negative-controls"
    negative.mkdir()
    baseline = {path.stem: json.loads(path.read_text(encoding="utf-8")) for path in results}

    def reject_record(name: str, suite: str, record: dict[str, object], expected_error: str) -> None:
        changed = negative / f"{name}.json"
        changed.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        inputs = [changed if path.stem == suite else path for path in results]
        rejection = run_writer(
            contract_path,
            artifact,
            inputs,
            negative / f"{name}-evidence.json",
            args.candidate_commit,
        )
        (negative / f"{name}.stderr").write_text(rejection.stderr, encoding="utf-8")
        if rejection.returncode == 0 or expected_error not in rejection.stderr:
            raise RuntimeError(f"{name} control was not rejected as expected: {rejection.stderr}")

    empty_selection = copy.deepcopy(baseline["sensitivity-baseline"])
    empty_selection["selected_cases"] = []
    reject_record("empty-case-selection", "sensitivity-baseline", empty_selection, "empty case selection")

    required_cases = contract.get("required_cases", [])
    for index, case_id in enumerate(required_cases, start=1):
        suite = next(
            item["id"]
            for item in contract["required_suites"]
            if case_id in item.get("required_cases", [])
        )
        omitted = copy.deepcopy(baseline[suite])
        omitted["selected_cases"] = [case for case in omitted["selected_cases"] if case != case_id]
        reject_record(f"omitted-case-{index}", suite, omitted, case_id)

    missing_observed_suite = "sensitivity-baseline"
    missing_observed_case = copy.deepcopy(baseline[missing_observed_suite])
    missing_observed_case["observed_cases"].pop()
    reject_record(
        "missing-observed-case",
        missing_observed_suite,
        missing_observed_case,
        "missing or unexpected observed cases",
    )

    skipped_case = copy.deepcopy(baseline["recovery"])
    skipped_case["skipped_tests"] = 1
    wrong_version = copy.deepcopy(baseline["recovery"])
    wrong_version["postgresql_version"] = "18.30"
    controls = (
        ("skipped-case", "recovery", skipped_case, "unexpected skipped tests"),
        ("wrong-server-version", "recovery", wrong_version, "PostgreSQL version differs from its server"),
    )
    for name, suite, record, expected_error in controls:
        reject_record(name, suite, record, expected_error)

    prestarted_server = copy.deepcopy(baseline["recovery"])
    server = prestarted_server["installation_observations"][0]["server"]
    server["postmaster_start_epoch"] = float(server["minimum_postmaster_start_epoch"]) - 1
    server["fresh_postmaster"] = False
    reject_record(
        "prestarted-server", "recovery", prestarted_server,
        "did not use a fresh PostgreSQL process",
    )

    identity_controls = (
        ("wrong-candidate-commit", "candidate_commit", "b" * 40, "different candidate commit"),
        ("wrong-artifact-id", "artifact_id", "linux-arm64", "references an unavailable artifact"),
        ("wrong-artifact-digest", "artifact_digest", "0" * 64, "artifact digest does not match"),
        ("wrong-platform", "platform", "linux-arm64", "wrong artifact or platform"),
        ("wrong-build-kind", "build_kind", "instrumented", "wrong build kind"),
        ("wrong-feature-scope", "feature_scope", "instrumented", "wrong feature scope"),
        ("wrong-suite-version", "suite_version", "0.0.0", "suite_version does not match"),
    )
    for name, field, value, expected_error in identity_controls:
        changed = copy.deepcopy(baseline["sensitivity-baseline"])
        changed[field] = value
        reject_record(name, "sensitivity-baseline", changed, expected_error)

    identity_mismatch = copy.deepcopy(baseline["sensitivity-baseline"])
    identity_mismatch["candidate_identity"]["feature_scope"] = "instrumented"
    reject_record(
        "wrong-artifact-candidate-identity",
        "sensitivity-baseline",
        identity_mismatch,
        "candidate identity does not match the artifact",
    )

    instrumented = copy.deepcopy(baseline["sensitivity-baseline"])
    instrumented["build_kind"] = "instrumented"
    instrumented["candidate_identity"]["build_kind"] = "instrumented"
    instrumented["candidate_identity"]["build_provenance"]["instrumentation"] = "asan"
    for observation in instrumented["installation_observations"]:
        observation["build_kind"] = "instrumented"
    reject_record("instrumented-result", "sensitivity-baseline", instrumented, "wrong build kind")

    for index, field in enumerate(contract["evidence"]["required_suite_fields"], start=1):
        missing_field = copy.deepcopy(baseline["sensitivity-baseline"])
        missing_field.pop(field, None)
        expected_error = (
            "unknown or duplicate suite result" if field == "suite_id" else "missing required evidence fields"
        )
        reject_record(f"missing-evidence-field-{index}", "sensitivity-baseline", missing_field, expected_error)

    unavailable = negative / "unavailable-runtime"
    unavailable.mkdir()
    unavailable_contract = copy.deepcopy(contract)
    unavailable_suite = next(item for item in unavailable_contract["required_suites"] if item["id"] == "recovery")
    unavailable_suite["command_argv"] = [
        "bash", "-c", "printf '%s\\n' 'Docker daemon unavailable for qualification'; exit 125",
    ]
    unavailable_suite["command"] = " ".join(unavailable_suite["command_argv"])
    unavailable_contract_path = unavailable / "qualification.json"
    unavailable_contract_path.write_text(json.dumps(unavailable_contract, indent=2) + "\n", encoding="utf-8")
    unavailable_result = unavailable / "recovery.json"
    runner = subprocess.run(
        [
            sys.executable, "scripts/run_release_suite.py",
            "--qualification", str(unavailable_contract_path), "--suite", "recovery",
            "--artifact", str(artifact), "--candidate-commit", args.candidate_commit,
            "--log", str(unavailable / "recovery.log"), "--result", str(unavailable_result),
        ],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    unavailable_record = json.loads(unavailable_result.read_text(encoding="utf-8"))
    if (
        runner.returncode == 0
        or unavailable_record["status"] == "passed"
        or "Docker daemon unavailable for qualification" not in (unavailable / "recovery.log").read_text(encoding="utf-8")
    ):
        raise RuntimeError("unavailable infrastructure control passed or lost its diagnostic")
    unavailable_inputs = [unavailable_result if path.stem == "recovery" else path for path in results]
    rejection = run_writer(
        unavailable_contract_path,
        artifact,
        unavailable_inputs,
        unavailable / "RELEASE-EVIDENCE.json",
        args.candidate_commit,
    )
    (unavailable / "writer.stderr").write_text(rejection.stderr, encoding="utf-8")
    if rejection.returncode == 0 or "unexpected skipped tests" not in rejection.stderr:
        raise RuntimeError(f"unavailable infrastructure was not rejected: {rejection.stderr}")

    manual_pass = subprocess.run(
        [
            sys.executable, "scripts/release_evidence.py", "--output", str(negative / "manual-pass.json"),
            "--version", "0.108.0", "--candidate-commit", args.candidate_commit,
            "--qualification", str(contract_path), "--artifact", str(artifact),
            "--suite", "sensitivity-baseline=passed",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    (negative / "manual-pass.stderr").write_text(manual_pass.stderr, encoding="utf-8")
    if manual_pass.returncode == 0 or "manually declared suite results" not in manual_pass.stderr:
        raise RuntimeError(f"manual suite pass declaration was not rejected: {manual_pass.stderr}")
    missing_shard = run_writer(
        contract_path, artifact, results[:1] + results[2:],
        negative / "missing-shard-evidence.json", args.candidate_commit,
    )
    (negative / "missing-shard.stderr").write_text(missing_shard.stderr, encoding="utf-8")
    if missing_shard.returncode == 0 or "required qualification shards are missing" not in missing_shard.stderr:
        raise RuntimeError(f"missing-shard control was not rejected: {missing_shard.stderr}")

    duplicate_shard = copy.deepcopy(baseline["recovery"])
    duplicate_shard["shard"]["id"] = baseline["sensitivity-baseline"]["shard"]["id"]
    reject_record(
        "duplicate-shard", "recovery", duplicate_shard,
        "unknown or duplicate qualification shard",
    )

    overlapping_contract = copy.deepcopy(contract)
    overlapping_shards = overlapping_contract["required_shards"]
    overlapping_case = overlapping_shards[0]["required_cases"][0]
    overlapping_shards[1]["required_cases"][0] = overlapping_case
    overlapping_contract_path = negative / "overlapping-shards-qualification.json"
    overlapping_contract_path.write_text(
        json.dumps(overlapping_contract, indent=2) + "\n", encoding="utf-8"
    )
    overlap = run_writer(
        overlapping_contract_path, artifact, results,
        negative / "overlapping-shards-evidence.json", args.candidate_commit,
    )
    (negative / "overlapping-shards.stderr").write_text(overlap.stderr, encoding="utf-8")
    if overlap.returncode == 0 or f"required case {overlapping_case!r} is assigned to multiple shards" not in overlap.stderr:
        raise RuntimeError(f"overlapping-shard control was not rejected: {overlap.stderr}")

    # Install A while declaring a distinct, still loadable B. The test cases
    # must pass, but neither the runner nor the evidence writer may accept B.
    wrong = output / "wrong-package"
    wrong_package = wrong / package.name
    shutil.copytree(package, wrong_package)
    control = wrong_package / "extension/pg_trickle.control"
    control.write_text(control.read_text(encoding="utf-8") + "\n# proof package B\n", encoding="utf-8")
    wrong_artifact = wrong / artifact.name
    archive_package(wrong_package, wrong_artifact)
    wrong_contract = copy.deepcopy(contract)
    wrong_suite = next(suite for suite in wrong_contract["required_suites"] if suite["id"] == "sensitivity-baseline")
    wrong_suite["command_argv"] = [
        "bash", "-c",
        f"PGT_EXTENSION_DIR={shlex.quote(str(source))} "
        "bash scripts/run_light_e2e_tests.sh --test e2e_sensitivity_baseline_tests",
    ]
    wrong_suite["command"] = " ".join(wrong_suite["command_argv"])
    wrong_contract["required_suites"] = [wrong_suite]
    wrong_contract["required_shards"] = [
        shard for shard in wrong_contract["required_shards"]
        if shard["suite_id"] == "sensitivity-baseline"
    ]
    wrong_contract["required_cases"] = wrong_contract["required_shards"][0]["required_cases"]
    wrong_contract["artifacts"][0]["support_tier"] = "build-only"
    wrong_contract_path = wrong / "qualification.json"
    wrong_contract_path.write_text(json.dumps(wrong_contract, indent=2) + "\n", encoding="utf-8")
    wrong_result = wrong / "sensitivity-baseline.json"
    attempt = subprocess.run(
        [
            sys.executable, "scripts/run_release_suite.py",
            "--qualification", str(wrong_contract_path), "--suite", "sensitivity-baseline",
            "--artifact", str(wrong_artifact), "--candidate-commit", args.candidate_commit,
            "--log", str(wrong / "sensitivity-baseline.log"), "--result", str(wrong_result),
        ],
        cwd=ROOT,
        env=environment,
        check=False,
    )
    record = json.loads(wrong_result.read_text(encoding="utf-8"))
    observation = record["installation_observations"][0]
    if (
        attempt.returncode == 0
        or record["status"] != "failed"
        or any(case["status"] != "passed" for case in record["observed_cases"])
        or observation["installed_files"] == record["candidate_payload"]["files"]
    ):
        raise RuntimeError("package A/B control did not retain passing cases and reject the wrong payload")
    rejection = run_writer(
        wrong_contract_path, wrong_artifact, [wrong_result],
        wrong / "RELEASE-EVIDENCE.json", args.candidate_commit,
    )
    (wrong / "writer.stderr").write_text(rejection.stderr, encoding="utf-8")
    if rejection.returncode == 0 or "payload" not in rejection.stderr:
        raise RuntimeError(f"evidence writer accepted package B: {rejection.stderr}")
    print(f"release proof controls passed: {evidence}")


if __name__ == "__main__":
    main()
