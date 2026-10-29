#!/usr/bin/env python3
"""Run the two bounded source-level DVM mutants from issue #1289."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILDER_IMAGE = "pg_trickle_builder:pg18"
PACKAGE_SO = "/artefacts/pg_trickle-pg18/usr/lib/postgresql/18/lib/pg_trickle.so"
INSTALLED_SO = "/usr/lib/postgresql/18/lib/pg_trickle.so"
INFRA_MARKERS = (
    "Cannot connect to the Docker daemon",
    "error during connect",
    "No such file or directory (os error 2)",
    "failed to resolve source metadata",
    "TLS handshake timeout",
    "connection refused",
    "testcontainers error",
    "permission denied while trying to connect to the docker api",
    "not found locally",
)


@dataclass(frozen=True)
class Mutant:
    name: str
    source: str
    anchor: str
    replacement: str
    test: str
    expected_sql: str
    mutant_sql: str
    assertion: str


MUTANTS = (
    Mutant(
        name="having-unknown-retraction",
        source="src/dvm/operators/filter.rs",
        anchor="let retraction = crate::aggregate_lifecycle::having_retraction_sql(&predicate_sql);",
        replacement='let retraction = format!("({predicate_sql}) IS FALSE");',
        test="test_diff_full_equivalence_having_unknown_transitions",
        expected_sql="IS NOT TRUE",
        mutant_sql="IS FALSE",
        assertion="M-HAVING-UNKNOWN-RETRACTION TRUE-to-UNKNOWN cycle",
    ),
    Mutant(
        name="count-null-delete",
        source="src/dvm/operators/aggregate.rs",
        anchor='format!("COUNT(CASE WHEN __pgt_action = \'D\'{filter_and} THEN {col} ELSE NULL END)"),',
        replacement='format!("COUNT(CASE WHEN __pgt_action = \'D\'{filter_and} THEN 1 ELSE NULL END)"),',
        test="test_diff_full_equivalence_count_composite_general",
        expected_sql="__pgt_action = 'D' THEN payload ELSE NULL END",
        mutant_sql='__pgt_action = \'D\' THEN 1 ELSE NULL END',
        assertion="M-COUNT-NULL-DELETE SQL-NULL payload deletion cycle",
    ),
)


class CampaignError(RuntimeError):
    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def replace_unique(source: str, anchor: str, replacement: str) -> str:
    count = source.count(anchor)
    if count != 1:
        raise CampaignError(
            "inconclusive", f"patch anchor occurs {count} times; expected exactly one"
        )
    return source.replace(anchor, replacement, 1)


def command(argv: list[str], cwd: Path, log_path: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(argv, cwd=cwd, env=env, text=True, capture_output=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(result.stdout + result.stderr, encoding="utf-8")
    return result


def require_command(
    argv: list[str], cwd: Path, log_path: Path, stage: str, env: dict[str, str] | None = None
) -> str:
    result = command(argv, cwd, log_path, env)
    output = result.stdout + result.stderr
    if result.returncode:
        status = "infrastructure" if any(marker.lower() in output.lower() for marker in INFRA_MARKERS) else "unbuildable"
        raise CampaignError(status, f"{stage} failed; see {log_path}")
    return result.stdout.strip()


def parse_sha(text: str) -> str:
    match = re.search(r"\b([0-9a-f]{64})\b", text)
    if not match:
        raise CampaignError("stale", f"could not read a SHA-256 digest from: {text!r}")
    return match.group(1)


def docker_exists(image: str, cwd: Path, log_path: Path) -> bool:
    result = command(["docker", "image", "inspect", image, "--format", "{{.Id}}"], cwd, log_path)
    return result.returncode == 0


def docker_platform() -> str:
    requested = os.environ.get("DOCKER_PLATFORM") or os.environ.get("DOCKER_DEFAULT_PLATFORM")
    if requested:
        return requested
    machine = platform.machine().lower()
    return "linux/amd64" if machine in ("x86_64", "amd64") else "linux/arm64"


def ensure_builder(artifacts: Path, platform_name: str) -> None:
    if docker_exists(BUILDER_IMAGE, ROOT, artifacts / "builder-inspect.log"):
        return
    require_command(
        [
            "docker", "build", "--platform", platform_name, "--provenance=false",
            "--tag", BUILDER_IMAGE, "--file", "tests/Dockerfile.builder", str(ROOT),
        ],
        ROOT,
        artifacts / "build-builder.log",
        "builder image build",
    )


def image_library_sha(image: str, path: str, artifacts: Path, label: str, cwd: Path) -> str:
    output = require_command(
        ["docker", "run", "--rm", "--entrypoint", "sha256sum", image, path],
        cwd,
        artifacts / f"{label}-sha256.log",
        f"{label} library digest",
    )
    return parse_sha(output)


def build_image(context: Path, tag: str, target: str | None, artifacts: Path, platform_name: str) -> None:
    argv = [
        "docker", "build", "--platform", platform_name, "--provenance=false",
        "--tag", tag, "--file", "tests/Dockerfile.e2e",
        "--build-arg", f"BUILDER_IMAGE={BUILDER_IMAGE}",
    ]
    if target:
        argv.extend(["--target", target])
    if docker_exists(tag, context, artifacts / f"{tag.replace(':', '-')}-preexistence.log"):
        raise CampaignError("stale", f"refusing to overwrite existing image tag {tag}")
    require_command(argv + [str(context)], context, artifacts / f"{tag.replace(':', '-')}-build.log", f"Docker image build {tag}")


def run_test(
    case: Mutant,
    image: str,
    library_sha: str,
    sql_fragment: str,
    artifacts: Path,
    run_id: str,
    phase: str,
) -> tuple[int, str]:
    case_dir = artifacts / phase / case.name
    evidence_dir = case_dir / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "PGT_DISABLE_NEXTEST": "1",
            "PGS_E2E_IMAGE": image,
            "PGT_E2E_RUN_ID": f"i1289-{run_id}-{phase}-{case.name}",
            "PGT_DVM_MUTATION_CASE": case.name,
            "PGT_DVM_MUTATION_EXPECTED_LIBRARY_SHA256": library_sha,
            "PGT_DVM_MUTATION_EXPECTED_SQL_FRAGMENT": sql_fragment,
            "PGT_DVM_MUTATION_ARTIFACT_DIR": str(evidence_dir),
        }
    )
    result = subprocess.run(
        [
            "bash", str(ROOT / "scripts/run_e2e_tests.sh"),
            "--test", "e2e_diff_full_equivalence_tests", case.test,
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
    )
    output = result.stdout + result.stderr
    (case_dir / "test-output.log").write_text(output, encoding="utf-8")
    return result.returncode, output


def evidence_is_valid(case: Mutant, evidence_dir: Path, expected_sha: str, expected_sql: str) -> bool:
    loaded_path = evidence_dir / "loaded-library.txt"
    route_path = evidence_dir / "target-refresh.txt"
    delta_path = evidence_dir / "target-delta.log"
    if not (loaded_path.is_file() and route_path.is_file() and delta_path.is_file()):
        return False
    loaded = loaded_path.read_text(encoding="utf-8")
    route = route_path.read_text(encoding="utf-8")
    delta = delta_path.read_text(encoding="utf-8")
    return (
        INSTALLED_SO in loaded
        and expected_sha in loaded
        and "library_symbol=pg_trickle" in loaded
        and "library_search_path=$libdir" in loaded
        and "load=success" in loaded
        and "action=DIFFERENTIAL" in route
        and "status=COMPLETED" in route
        and "was_full_fallback=false" in route
        and re.search(r"delta_row_count=[1-9][0-9]*", route) is not None
        and "effective_refresh_mode=DIFFERENTIAL" in route
        and expected_sql in delta
    )


def infra_failure(output: str) -> bool:
    return any(marker.lower() in output.lower() for marker in INFRA_MARKERS)


def test_outcome(case: Mutant, returncode: int, output: str, evidence_ok: bool) -> tuple[str, str | None]:
    if evidence_ok and returncode and f"EXACT ORACLE INVARIANT VIOLATION in {case.assertion}" in output:
        counts = re.search(
            r"Counts: actual=(-?\d+), expected=(-?\d+), extra=(-?\d+), missing=(-?\d+)",
            output,
        )
        if counts and (int(counts.group(3)) > 0 or int(counts.group(4)) > 0):
            return "detected", f"EXACT ORACLE INVARIANT VIOLATION in {case.assertion}"
    if returncode == 0 and evidence_ok:
        return "survived", None
    if infra_failure(output):
        return "infrastructure", None
    if "error[E" in output or "could not compile" in output:
        return "unbuildable", None
    return "inconclusive", None


def worktree_patch(
    case: Mutant, commit: str, run_dir: Path, artifacts: Path
) -> tuple[Path, str, str, str]:
    worktree = run_dir / case.name
    require_command(
        ["git", "worktree", "add", "--detach", str(worktree), commit],
        ROOT,
        artifacts / f"{case.name}-worktree.log",
        f"{case.name} isolated worktree creation",
    )
    target = worktree / case.source
    source = target.read_text(encoding="utf-8")
    mutated = replace_unique(source, case.anchor, case.replacement)
    target.write_text(mutated, encoding="utf-8")

    names = subprocess.check_output(["git", "diff", "--name-only"], cwd=worktree, text=True).splitlines()
    if names != [case.source]:
        raise CampaignError("inconclusive", f"unexpected mutated worktree paths: {names}")
    patch = subprocess.check_output(["git", "diff", "--binary", "--", case.source], cwd=worktree)
    patch_path = artifacts / f"{case.name}.patch"
    patch_path.write_bytes(patch)
    require_command(
        ["git", "diff", "--check"],
        worktree,
        artifacts / f"{case.name}-diff-check.log",
        f"{case.name} patch validation",
    )
    return (
        worktree,
        sha256(patch),
        sha256(source.encode("utf-8")),
        sha256(mutated.encode("utf-8")),
    )


def write_summary(artifacts: Path, payload: dict) -> None:
    (artifacts / "outcomes.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    rows = [
        "| Mutant | Outcome | Evidence |",
        "|---|---|---|",
        "| M-IS-DELETE-SIGN (superseded) | inconclusive | INTERSECT is FullOnly; no admitted incremental path |",
        "| M-IS-ZERO-BOUNDARY (superseded) | inconclusive | INTERSECT is FullOnly; no admitted incremental path |",
    ]
    for result in payload.get("mutants", []):
        rows.append(
            f"| {result['name']} | {result['status']} | {result.get('artifacts', 'see outcomes.json')} |"
        )
    text = (
        "# Issue #1289 source-level DVM mutation pilot\n\n"
        f"Candidate: `{payload.get('candidate_commit', 'unknown')}`\n\n"
        "The superseded INTERSECT candidates were not built or counted as kills.\n\n"
        + "\n".join(rows)
        + "\n"
    )
    (artifacts / "outcomes.md").write_text(text, encoding="utf-8")


def self_test() -> None:
    for case in MUTANTS:
        source = (ROOT / case.source).read_text(encoding="utf-8")
        changed = replace_unique(source, case.anchor, case.replacement)
        assert changed != source
        assert case.replacement in changed
        assert source.count(case.anchor) == 1
    for source in ("", "anchor anchor"):
        try:
            replace_unique(source, "anchor", "replacement")
        except CampaignError:
            pass
        else:
            raise AssertionError("non-unique patch anchor must fail closed")
    exact_mismatch = (
        "EXACT ORACLE INVARIANT VIOLATION in M-HAVING-UNKNOWN-RETRACTION TRUE-to-UNKNOWN cycle\n"
        "Counts: actual=0, expected=1, extra=0, missing=1"
    )
    assert test_outcome(MUTANTS[0], 101, exact_mismatch, True)[0] == "detected"
    assert test_outcome(MUTANTS[0], 101, "assertion failed", True)[0] == "inconclusive"
    print("mutation patch self-test passed")


def run_campaign(artifacts: Path) -> int:
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    if status.strip():
        raise CampaignError("inconclusive", "campaign requires a clean candidate worktree")
    candidate = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    run_id = datetime.now(timezone.utc).strftime("%y%m%d%H%M%S")
    platform_name = docker_platform()
    artifacts.mkdir(parents=True, exist_ok=False)
    payload: dict = {
        "candidate_commit": candidate,
        "target": "main",
        "platform": platform_name,
        "retry_count": 0,
        "superseded_intersect_mutants": {
            "M-IS-DELETE-SIGN": {"status": "inconclusive", "reason": "INTERSECT is FullOnly"},
            "M-IS-ZERO-BOUNDARY": {"status": "inconclusive", "reason": "INTERSECT is FullOnly"},
        },
        "baselines": [],
        "mutants": [],
    }
    write_summary(artifacts, payload)
    ensure_builder(artifacts, platform_name)

    baseline_tag = f"pg_trickle_e2e:i1289-{run_id}-baseline"
    baseline_build_tag = f"pg_trickle_dvm_build:i1289-{run_id}-baseline"
    baseline_artifacts = artifacts / "baseline-image"
    baseline_artifacts.mkdir()
    build_image(ROOT, baseline_build_tag, "builder", baseline_artifacts, platform_name)
    produced_baseline = image_library_sha(baseline_build_tag, PACKAGE_SO, baseline_artifacts, "produced", ROOT)
    build_image(ROOT, baseline_tag, None, baseline_artifacts, platform_name)
    installed_baseline = image_library_sha(baseline_tag, INSTALLED_SO, baseline_artifacts, "installed", ROOT)
    if produced_baseline != installed_baseline:
        raise CampaignError("stale", "baseline package and installed library hashes differ")
    payload["baseline_image"] = {
        "tag": baseline_tag,
        "image_id": subprocess.check_output(["docker", "image", "inspect", baseline_tag, "--format", "{{.Id}}"], cwd=ROOT, text=True).strip(),
        "produced_library_sha256": produced_baseline,
        "installed_library_sha256": installed_baseline,
    }

    # Both named unmodified cases must pass before any mutant is built.
    for case in MUTANTS:
        code, output = run_test(case, baseline_tag, installed_baseline, case.expected_sql, artifacts, run_id, "baseline")
        evidence_dir = artifacts / "baseline" / case.name / "evidence"
        valid = evidence_is_valid(case, evidence_dir, installed_baseline, case.expected_sql)
        payload["baselines"].append(
            {"test": case.test, "exit_code": code, "route_evidence_valid": valid, "output": f"baseline/{case.name}/test-output.log"}
        )
        write_summary(artifacts, payload)
        if code != 0 or not valid:
            reason = "baseline failed" if code else "baseline route evidence missing"
            status = "infrastructure" if infra_failure(output) else "inconclusive"
            for pending in MUTANTS:
                payload["mutants"].append({"name": pending.name, "status": status, "reason": reason})
            write_summary(artifacts, payload)
            return 1

    run_dir = artifacts / "worktrees"
    run_dir.mkdir()
    for case in MUTANTS:
        record = {"name": case.name, "test": case.test}
        try:
            worktree, patch_sha, baseline_source_sha, source_sha = worktree_patch(
                case, candidate, run_dir, artifacts
            )
            record.update(
                {
                    "patch_sha256": patch_sha,
                    "baseline_source_sha256": baseline_source_sha,
                    "mutated_source_sha256": source_sha,
                }
            )
            build_tag = f"pg_trickle_dvm_build:i1289-{run_id}-{case.name}"
            image_tag = f"pg_trickle_e2e:i1289-{run_id}-{case.name}"
            build_dir = artifacts / case.name / "build"
            build_dir.mkdir(parents=True)
            build_image(worktree, build_tag, "builder", build_dir, platform_name)
            produced = image_library_sha(build_tag, PACKAGE_SO, build_dir, "produced", worktree)
            build_image(worktree, image_tag, None, build_dir, platform_name)
            installed = image_library_sha(image_tag, INSTALLED_SO, build_dir, "installed", worktree)
            record.update(
                {
                    "image": image_tag,
                    "image_id": subprocess.check_output(["docker", "image", "inspect", image_tag, "--format", "{{.Id}}"], cwd=ROOT, text=True).strip(),
                    "produced_library_sha256": produced,
                    "installed_library_sha256": installed,
                }
            )
            if produced != installed or installed == installed_baseline:
                record.update({"status": "stale", "reason": "mutant library identity does not match a distinct package"})
                payload["mutants"].append(record)
                write_summary(artifacts, payload)
                continue

            code, output = run_test(case, image_tag, installed, case.mutant_sql, artifacts, run_id, "mutant")
            evidence_dir = artifacts / "mutant" / case.name / "evidence"
            valid = evidence_is_valid(case, evidence_dir, installed, case.mutant_sql)
            outcome, assertion = test_outcome(case, code, output, valid)
            record.update(
                {
                    "status": outcome,
                    "route_evidence_valid": valid,
                    "first_detecting_assertion": assertion,
                    "output": f"mutant/{case.name}/test-output.log",
                    "artifacts": f"mutant/{case.name}/evidence/",
                }
            )
            if outcome == "detected":
                evidence_dir.joinpath("expected-actual-bags.txt").write_text(output, encoding="utf-8")
            payload["mutants"].append(record)
            write_summary(artifacts, payload)
        except CampaignError as error:
            record.update({"status": error.status, "reason": str(error)})
            payload["mutants"].append(record)
            write_summary(artifacts, payload)
        finally:
            if (run_dir / case.name).exists():
                subprocess.run(["git", "worktree", "remove", "--force", str(run_dir / case.name)], cwd=ROOT, check=False, capture_output=True)

    return 0 if all(item.get("status") == "detected" for item in payload["mutants"]) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="assert patch anchors are unique and fail closed")
    parser.add_argument("--artifacts-dir", type=Path, help="artifact destination (defaults under ~/.p2p/work)")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifacts = args.artifacts_dir or Path.home() / ".p2p" / "work" / "issue-1289-dvm-mutation-pilot" / run_id
    try:
        result = run_campaign(artifacts)
    except CampaignError as error:
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "campaign-error.txt").write_text(f"{error.status}: {error}\n", encoding="utf-8")
        print(f"campaign {error.status}: {error}", file=sys.stderr)
        return 1
    print(f"campaign artifacts: {artifacts}")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
