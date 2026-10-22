#!/usr/bin/env python3
"""Qualify an executable missing-retraction SQL mutant without editing production."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = ROOT / ".p2p/work/issue-1223-join-checker/evidence"
SOURCE = ROOT / "verification/join_plan.rs"
TEST = ROOT / "tests/dvm_aggregate_execution_tests.rs"
CASE = "test_join_endpoint_sql_preserves_images_retractions_and_bag_duplicates"


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def run(target: str, log: Path) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["./scripts/run_dvm_integration_tests.sh", "pg18", target,
         "-E", f"test(={CASE})", "--retries", "0"],
        cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600,
    )
    log.write_text(result.stdout, encoding="utf-8")
    return result


def main() -> int:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    record = EVIDENCE / "join-sql-control.json"
    record.unlink(missing_ok=True)
    original = SOURCE.read_text(encoding="utf-8")
    before = "    for term in [delete, insert] {"
    if original.count(before) != 1:
        raise ValueError("expected one executable endpoint-term loop")
    mutant = original.replace(before, "    for term in [insert, insert] {", 1)
    module = ROOT / ".p2p/tmp/issue-1223-join-sql/mutant.rs"
    target = ROOT / "tests/__pgt_join_sql_mutant.rs"
    native = TEST.read_text(encoding="utf-8")
    import_path = '#[path = "../verification/join_plan.rs"]'
    if native.count(import_path) != 1 or module.exists() or target.exists():
        raise ValueError("native import moved or temporary control files already exist")
    baseline = run("dvm_aggregate_execution_tests", EVIDENCE / "join-sql-baseline.log")
    if baseline.returncode or CASE not in baseline.stdout or "1 passed" not in baseline.stdout:
        raise RuntimeError("native endpoint baseline did not execute and pass")
    owned = []
    try:
        module.parent.mkdir(parents=True, exist_ok=True)
        for path, contents in [
            (module, mutant),
            (target, native.replace(import_path, '#[path = "../.p2p/tmp/issue-1223-join-sql/mutant.rs"]', 1)),
        ]:
            with path.open("x", encoding="utf-8") as output:
                output.write(contents)
            owned.append(path)
        result = run("__pgt_join_sql_mutant", EVIDENCE / "join-sql-missing-retraction.log")
        if result.returncode == 0 or "test result: FAILED." not in result.stdout \
                or "endpoint bag/image actions differ" not in result.stdout \
                or "assertion `left == right` failed" not in result.stdout:
            raise RuntimeError("SQL mutant did not fail the intended exact-action comparison")
        if SOURCE.read_text(encoding="utf-8") != original:
            raise RuntimeError("production source changed during SQL qualification")
        record.write_text(json.dumps({
            "schema": "pg-trickle-join-sql-control-v1", "source_sha256": digest(original.encode()),
            "mutant_source_sha256": digest(mutant.encode()), "test_source_sha256": digest(native.encode()),
            "runtime_case": CASE, "retries": 0,
            "baseline": {"returncode": baseline.returncode, "output_sha256": digest(baseline.stdout.encode())},
            "mutant": {"returncode": result.returncode, "failure_kind": "multiset_mismatch",
                       "phase": "exact_endpoint_actions", "output_sha256": digest(result.stdout.encode())},
            "boundary": "The actual SQL adapter is compiled from a mutant source copy; production source and release packages are unchanged.",
        }, indent=2) + "\n", encoding="utf-8")
        print(f"Endpoint SQL baseline passes; missing retraction fails exact actions. Evidence: {record.relative_to(ROOT)}")
        return 0
    finally:
        for path in reversed(owned):
            path.unlink()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"join SQL qualification failed closed: {error}", file=sys.stderr)
        sys.exit(1)
