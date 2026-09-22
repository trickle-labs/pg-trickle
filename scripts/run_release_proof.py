#!/usr/bin/env python3
"""Exercise the release runner and evidence writer on a packaged PR candidate."""

from __future__ import annotations

import argparse
import copy
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

    negative = output / "negative-controls"
    negative.mkdir()
    baseline = {path.stem: json.loads(path.read_text(encoding="utf-8")) for path in results}
    missing_case = copy.deepcopy(baseline["sensitivity-baseline"])
    missing_case["observed_cases"].pop()
    skipped_case = copy.deepcopy(baseline["recovery"])
    skipped_case["skipped_tests"] = 1
    wrong_version = copy.deepcopy(baseline["recovery"])
    wrong_version["postgresql_version"] = "18.30"
    controls = (
        ("missing-case", "sensitivity-baseline", missing_case, "missing or unexpected observed cases"),
        ("skipped-case", "recovery", skipped_case, "unexpected skipped tests"),
        ("wrong-server-version", "recovery", wrong_version, "PostgreSQL version differs from its server"),
    )
    for name, suite, record, expected_error in controls:
        changed = negative / f"{name}.json"
        changed.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        inputs = [changed if path.stem == suite else path for path in results]
        rejection = run_writer(
            contract_path, artifact, inputs, negative / f"{name}-evidence.json", args.candidate_commit
        )
        (negative / f"{name}.stderr").write_text(rejection.stderr, encoding="utf-8")
        if rejection.returncode == 0 or expected_error not in rejection.stderr:
            raise RuntimeError(f"{name} control was not rejected as expected: {rejection.stderr}")
    missing_shard = run_writer(
        contract_path, artifact, results[:1] + results[2:],
        negative / "missing-shard-evidence.json", args.candidate_commit,
    )
    (negative / "missing-shard.stderr").write_text(missing_shard.stderr, encoding="utf-8")
    if missing_shard.returncode == 0 or "required qualification shards are missing" not in missing_shard.stderr:
        raise RuntimeError(f"missing-shard control was not rejected: {missing_shard.stderr}")

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
