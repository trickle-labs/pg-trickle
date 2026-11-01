#!/usr/bin/env python3
"""Prove packaged INTERSECT refresh detects two source-level semantic mutants."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "src/dvm/operators/intersect.rs"
EVIDENCE = ROOT / "target/intersect-semantic-mutants"
IMAGE_PREFIX = "pg_trickle_intersect_qualification"
BUILDER_IMAGE = "pg_trickle_builder:pg18"


def docker_host_platform() -> str:
    machine = os.uname().machine
    architecture = {"x86_64": "amd64", "aarch64": "arm64"}.get(machine, machine)
    return os.environ.get("DOCKER_DEFAULT_PLATFORM", f"linux/{architecture}")

DELETE_SIGN = (
    "SUM(CASE WHEN __pgt_action = 'I' THEN 1::bigint ELSE -1::bigint END)"
)
VISIBILITY = (
    "AND new_count_l > 0 AND new_count_r > 0 THEN 'I'"
)
CASES = {
    "baseline": (
        "test_intersect_differential_presence_transitions_and_private_counts",
        "test_intersect_differential_empty_invisible_rows_simultaneous_changes_and_noop",
    ),
    "delete-sign": (
        "test_intersect_differential_presence_transitions_and_private_counts",
    ),
    "visibility-threshold": (
        "test_intersect_differential_empty_invisible_rows_simultaneous_changes_and_noop",
    ),
}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run(command: list[str], *, env: dict[str, str] | None = None, log: Path) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    log.write_text(result.stdout, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}); see {log}: {' '.join(command)}")
    return result.stdout


def ensure_builder_image() -> str:
    inspect = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", BUILDER_IMAGE],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if inspect.returncode == 0:
        return inspect.stdout.strip()
    pulled = subprocess.run(
        ["docker", "pull", "ghcr.io/trickle-labs/pg-trickle/builder:pg18"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if pulled.returncode == 0:
        run(
            ["docker", "tag", "ghcr.io/trickle-labs/pg-trickle/builder:pg18", BUILDER_IMAGE],
            log=EVIDENCE / "builder-tag.log",
        )
        inspect = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", BUILDER_IMAGE],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if inspect.returncode:
            raise RuntimeError("pulled builder image cannot be inspected")
        return inspect.stdout.strip()
    run(
        ["docker", "build", "--platform", docker_host_platform(), "-f",
         "tests/Dockerfile.builder", "-t", BUILDER_IMAGE, "."],
        log=EVIDENCE / "builder-build.log",
    )
    return docker_host_platform()


def build_image(
    name: str, source_bytes: bytes, mutant_source: bytes | None, platform: str
) -> dict[str, str]:
    image = f"{IMAGE_PREFIX}:{name}"
    if mutant_source is not None:
        SOURCE.write_bytes(mutant_source)
    try:
        run(
            ["docker", "build", "--platform", platform, "-t", image, "-f", "tests/Dockerfile.e2e",
             "--build-arg", f"BUILDER_IMAGE={BUILDER_IMAGE}", "."],
            log=EVIDENCE / f"{name}-image-build.log",
        )
    finally:
        SOURCE.write_bytes(source_bytes)
    if SOURCE.read_bytes() != source_bytes:
        raise RuntimeError("production INTERSECT source was not restored after image build")
    image_id = run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        log=EVIDENCE / f"{name}-image-id.log",
    ).strip()
    image_library_hash = run(
        ["docker", "run", "--pull=never", "--platform", platform, "--rm", "--entrypoint", "sha256sum", image_id,
         "/usr/lib/postgresql/18/lib/pg_trickle.so"],
        log=EVIDENCE / f"{name}-image-library.log",
    ).split()[0]
    return {
        "image": image,
        "image_id": image_id,
        "platform": platform,
        "library_sha256": image_library_hash,
    }


def run_case(image: str, platform: str, case: str, name: str, expected_mutant: bool) -> str:
    command = [
        "./scripts/run_e2e_tests.sh",
        "--test", "e2e_set_operation_tests",
        "-E", f"test(={case})",
        "--retries", "0",
        "--no-capture",
    ]
    env = os.environ.copy()
    env["PGS_E2E_IMAGE"] = image
    env["PGS_E2E_PLATFORM"] = platform
    env["PGT_E2E_RUN_ID"] = f"intersect-{name}-{os.getpid()}"
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    log = EVIDENCE / f"{name}-{case}.log"
    log.write_text(result.stdout, encoding="utf-8")
    if expected_mutant:
        if result.returncode == 0 or "EXACT ORACLE INVARIANT VIOLATION" not in result.stdout:
            raise RuntimeError(f"{name}: mutant did not fail the exact result oracle; see {log}")
        if case not in result.stdout or "Counts: actual=" not in result.stdout:
            raise RuntimeError(f"{name}: failure was not the intended bag-result mismatch; see {log}")
    elif result.returncode or "1 passed" not in result.stdout:
        raise RuntimeError(f"{name}: candidate baseline failed; see {log}")
    return digest(result.stdout.encode())


def main() -> int:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    for old in EVIDENCE.iterdir():
        if old.is_file():
            old.unlink()
    platform = ensure_builder_image()

    original = SOURCE.read_bytes()
    source = original.decode("utf-8")
    if source.count(DELETE_SIGN) != 3:
        raise RuntimeError("expected the two branch delete-sign expressions in intersect.rs")
    if source.count(VISIBILITY) != 1:
        raise RuntimeError("expected one zero-to-positive visibility condition in intersect.rs")

    right_branch_start = source.index(DELETE_SIGN, source.index(DELETE_SIGN) + len(DELETE_SIGN))
    wrong_sign = source[:right_branch_start] + source[right_branch_start:].replace(
        DELETE_SIGN, DELETE_SIGN.replace("ELSE -1::bigint", "ELSE 1::bigint"), 1
    )
    wrong_threshold = source.replace(
        VISIBILITY,
        VISIBILITY.replace("new_count_r > 0", "new_count_r >= 0"),
        1,
    )
    candidates = {
        "baseline": (None, False),
        "delete-sign": (wrong_sign.encode(), True),
        "visibility-threshold": (wrong_threshold.encode(), True),
    }
    candidate_commit = run(["git", "rev-parse", "HEAD"], log=EVIDENCE / "candidate-commit.log").strip()
    records: dict[str, object] = {
        "schema": "pg-trickle-intersect-semantic-mutants-v1",
        "candidate_commit": candidate_commit,
        "production_source_sha256": digest(original),
        "mutations": {},
    }
    try:
        for name, (mutant, should_fail) in candidates.items():
            image_record = build_image(name, original, mutant, platform)
            case_hashes = [
                run_case(image_record["image"], image_record["platform"], case, name, should_fail)
                for case in CASES[name]
            ]
            records["mutations"][name] = {
                **image_record,
                "mutant_source_sha256": digest(mutant) if mutant is not None else None,
                "case_output_sha256": case_hashes,
                "expected_outcome": "exact_bag_mismatch" if should_fail else "pass",
            }
    finally:
        SOURCE.write_bytes(original)
    if SOURCE.read_bytes() != original:
        raise RuntimeError("INTERSECT production source changed during mutation qualification")

    (EVIDENCE / "summary.json").write_text(
        json.dumps(records, indent=2) + "\n", encoding="utf-8"
    )
    print(f"INTERSECT baseline and both exact-result mutants qualified: {EVIDENCE / 'summary.json'}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"INTERSECT semantic mutation qualification failed closed: {error}", file=sys.stderr)
        sys.exit(1)
