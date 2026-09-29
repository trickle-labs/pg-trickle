#!/usr/bin/env python3
"""Run the pinned Verus proof and require a semantic mutation to fail closed."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "verification/lsn.rs"
IMAGE = (
    "ghcr.io/verus-lang/verus:0.2025.06.23.2e59154"
    "@sha256:c4d0471379b23c3c6f52e3d7226c7dad28f488c4288e14c623002bd279a745e0"
)
REQUIRED_OBLIGATIONS = (
    "lsn_parse_value_and_bounds",
    "lsn_numeric_order",
    "lsn_format_parse_roundtrip",
)
VERIFICATION_SUMMARY = re.compile(r"verification results::\s+(\d+) verified,\s+(\d+) errors")
EVIDENCE = ROOT / ".p2p/work/issue-1097-verified-production-lsn/evidence"
PROOF_RECORD = EVIDENCE / "lsn-verification.json"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def docker_prefix() -> list[str]:
    return [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "-v",
        f"{ROOT}:/workspace",
        "-w",
        "/workspace",
        IMAGE,
    ]


def command_for(source: str) -> list[str]:
    return [
        *docker_prefix(),
        "verus",
        "--triggers-mode",
        "silent",
        source,
    ]


def mutate_executable_source(source: str) -> str:
    original = "high * 4_294_967_296_u64 + low"
    mutant = "high * 4_294_967_296_u64 + (low + 1)"
    if source.count(original) != 1:
        raise ValueError("expected exactly one executable pack_halves expression")
    return source.replace(original, mutant, 1)


def run_verus(source_path: str, log_path: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command_for(source_path),
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=300,
    )
    log_path.write_text(result.stdout, encoding="utf-8")
    return result


def run_tool_identity(tool: str, arguments: list[str], log_path: Path) -> str:
    command = [*docker_prefix(), tool, *arguments]
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=60,
    )
    log_path.write_text(result.stdout, encoding="utf-8")
    identity = result.stdout.strip()
    if result.returncode != 0 or not identity:
        raise RuntimeError(f"could not establish pinned {tool} identity; see {log_path.relative_to(ROOT)}")
    return identity


def check_contract() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    for obligation in REQUIRED_OBLIGATIONS:
        if not re.search(rf"\bpub fn\s+{obligation}\b", source):
            raise ValueError(f"required executable Verus obligation is missing: {obligation}")
    if "--verify" in " ".join(command_for("verification/lsn.rs")):
        raise ValueError("Verus command contains an unsupported --verify option")
    mutant = mutate_executable_source(source)
    if mutant == source:
        raise ValueError("semantic mutation did not change executable parser source")
    library = (ROOT / "src/lib.rs").read_text(encoding="utf-8")
    if '#[path = "../verification/lsn.rs"]' not in library:
        raise ValueError("production module does not import the verifier source")
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    if "  lsn-verification:" not in workflow:
        raise ValueError("CI does not define the required LSN verification job")
    if "python3 scripts/check_lsn_verification.py" not in workflow:
        raise ValueError("CI does not invoke the fail-closed proof runner")
    if "needs: [lsn-verification]" not in workflow:
        raise ValueError("candidate package build is not gated by LSN verification")
    if f"docker pull {IMAGE}" not in workflow:
        raise ValueError("CI does not pull the exact pinned Verus image before --pull=never")
    proof_job = workflow.split("  lsn-verification:", 1)[1].split("  # ── Unit tests", 1)[0]
    if "if:" in proof_job or "continue-on-error:" in proof_job:
        raise ValueError("CI proof job can be conditionally skipped or ignored")
    if "run: python3 scripts/check_lsn_verification.py\n" not in proof_job:
        raise ValueError("CI proof job does not run the fail-closed verifier command exactly")


def run_checks() -> None:
    check_contract()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    if PROOF_RECORD.exists():
        previous_record = PROOF_RECORD.read_bytes()
        previous_digest = sha256(previous_record)
        PROOF_RECORD.unlink()
        archive_dir = ROOT / ".p2p/work/issue-1097-verified-production-lsn/history" / previous_digest
        archive_dir.mkdir(parents=True, exist_ok=True)
        (archive_dir / "lsn-verification.json").write_bytes(previous_record)
    source = SOURCE.read_text(encoding="utf-8")
    source_digest = sha256(source.encode("utf-8"))
    baseline_log = EVIDENCE / "lsn-verification-baseline.log"
    mutant_log = EVIDENCE / "lsn-verification-mutant.log"
    compiler_log = EVIDENCE / "lsn-verus-version.log"
    solver_log = EVIDENCE / "lsn-z3-version.log"
    temp_dir = ROOT / ".p2p/tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    mutant_path = temp_dir / "lsn-semantic-mutant.rs"
    try:
        compiler_identity = run_tool_identity("verus", ["--version"], compiler_log)
        solver_identity = run_tool_identity("z3", ["--version"], solver_log)
        baseline = run_verus("verification/lsn.rs", baseline_log)
        result_summary = VERIFICATION_SUMMARY.search(baseline.stdout)
        verified_count = int(result_summary.group(1)) if result_summary else 0
        errors_count = int(result_summary.group(2)) if result_summary else 0
        if (
            baseline.returncode != 0
            or result_summary is None
            or verified_count < len(REQUIRED_OBLIGATIONS)
            or errors_count != 0
        ):
            raise RuntimeError(
                "pinned Verus baseline failed or omitted its verification summary; "
                f"see {baseline_log.relative_to(ROOT)}"
            )
        mutant_source = mutate_executable_source(source)
        mutant_path.write_text(mutant_source, encoding="utf-8")
        mutant = run_verus(".p2p/tmp/lsn-semantic-mutant.rs", mutant_log)
        mutation_summary = VERIFICATION_SUMMARY.search(mutant.stdout)
        mutation_errors = int(mutation_summary.group(2)) if mutation_summary else 0
        if mutant.returncode == 0 or mutation_summary is None or mutation_errors == 0:
            raise RuntimeError(
                "pinned Verus did not report a rejected executable semantic mutation; "
                f"see {mutant_log.relative_to(ROOT)}"
            )
        record = {
            "schema": "pg-trickle-lsn-verification-v1",
            "source": "verification/lsn.rs",
            "source_sha256": source_digest,
            "verus_image": IMAGE,
            "compiler_identity": compiler_identity,
            "solver_identity": solver_identity,
            "command": command_for("verification/lsn.rs")[1:],
            "obligations": list(REQUIRED_OBLIGATIONS),
            "baseline": {
                "returncode": baseline.returncode,
                "summary": result_summary.group(0),
                "output_sha256": sha256(baseline.stdout.encode("utf-8")),
            },
            "semantic_mutation": {
                "source_sha256": sha256(mutant_source.encode("utf-8")),
                "returncode": mutant.returncode,
                "output_sha256": sha256(mutant.stdout.encode("utf-8")),
            },
        }
        PROOF_RECORD.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        print(f"Verus baseline passed; semantic mutation rejected. Evidence: {PROOF_RECORD.relative_to(ROOT)}")
    finally:
        mutant_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract-only", action="store_true")
    args = parser.parse_args()
    try:
        if args.contract_only:
            check_contract()
            print("LSN proof source, mutation, and CI gate contract checks passed")
        else:
            run_checks()
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"LSN verification failed closed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
