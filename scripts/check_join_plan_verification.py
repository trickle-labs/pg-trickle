#!/usr/bin/env python3
"""Verify the production endpoint checker and conservative scope decisions."""

import argparse
import json
import re
import subprocess
import sys

import check_lsn_verification as runner

SOURCE = runner.ROOT / "verification/join_plan.rs"
EVIDENCE = runner.ROOT / ".p2p/work/issue-1223-join-checker/evidence"
OBLIGATIONS = ["admitted_equality_type", "check_endpoint_plan", "scope_pair", "unchanged_pair_has_zero_difference"]
PRODUCTION_INPUTS = [
    "src/lib.rs", "src/api/mod.rs", "src/api/helpers.rs", "src/api/refresh_ops.rs", "src/api/security_context.rs",
    "src/dvm/parser/validation.rs", "src/dvm/parser/mod.rs", "src/dvm/parser/sublinks/mod.rs", "src/cdc/mod.rs",
    "src/dvm/mod.rs", "src/dvm/diff.rs", "src/dvm/operators/join.rs",
    "src/dvm/operators/join_common.rs", "src/dvm/operators/project.rs", "src/dvm/operators/scan.rs",
    "src/refresh/codegen.rs", "src/refresh/merge/mod.rs", "src/hash.rs",
    "verification/cdc_elision.rs", "Cargo.toml", "Cargo.lock",
]
MUTATIONS = {
    "unknown-equality-type": ("    oid == 16 || oid == 20 || oid == 21 || oid == 23 || oid == 2950\n", "    true\n"),
    "lost-right-scope": ("    left_changed || right_changed\n", "    left_changed\n"),
    "mixed-snapshot-admission": (
        "        && matches!(insert.left_boundary, Boundary::After) && matches!(insert.right_boundary, Boundary::After)\n",
        "        && matches!(insert.left_boundary, Boundary::After)\n",
    ),
    "swapped-binding-admission": (
        "    left_scan != right_scan\n        && insert.left_scan == left_scan",
        "    left_scan != right_scan\n        && true",
    ),
    "wrong-sign-admission": ("        && !insert.retract && delete.retract\n", "        && !insert.retract\n"),
}


def mutate(source: str, name: str) -> str:
    before, after = MUTATIONS[name]
    if source.count(before) != 1:
        raise ValueError(f"{name}: expected exactly one executable decision")
    return source.replace(before, after, 1)


def check_contract() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    for name in OBLIGATIONS:
        if not re.search(rf"pub (?:proof )?fn {name}\b", source):
            raise ValueError(f"missing join obligation: {name}")
    for name in MUTATIONS:
        mutate(source, name)
    if '#[path = "../verification/join_plan.rs"]' not in (runner.ROOT / "src/lib.rs").read_text():
        raise ValueError("production does not import the checked join kernel")
    caller = (runner.ROOT / "src/dvm/operators/join.rs").read_text()
    for name in ["endpoint_changes_sql", "scope_pair", "admitted_equality_type"]:
        if f"crate::join_plan::{name}(" not in caller:
            raise ValueError(f"production bypasses {name}")
    if "check_endpoint_plan(0, 1, insert, delete)" not in source:
        raise ValueError("SQL lowering bypasses the checked endpoint plan")


def run_checks() -> None:
    check_contract()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    temporary = runner.ROOT / ".p2p/tmp/join-plan-mutant.rs"
    temporary.parent.mkdir(parents=True, exist_ok=True)
    source = SOURCE.read_text(encoding="utf-8")
    try:
        compiler = runner.run_tool_identity("verus", ["--version"], EVIDENCE / "join-verus-version.log")
        solver = runner.run_tool_identity("z3", ["--version"], EVIDENCE / "join-z3-version.log")
        baseline = runner.run_verus("verification/join_plan.rs", EVIDENCE / "join-verification-baseline.log")
        summary = runner.VERIFICATION_SUMMARY.search(baseline.stdout)
        if baseline.returncode or summary is None or int(summary[1]) < 6 or int(summary[2]) != 0:
            raise RuntimeError("join baseline failed or omitted required obligations")
        controls = {}
        for name in MUTATIONS:
            mutant = mutate(source, name)
            temporary.write_text(mutant, encoding="utf-8")
            result = runner.run_verus(".p2p/tmp/join-plan-mutant.rs", EVIDENCE / f"join-mutant-{name}.log")
            rejected = runner.VERIFICATION_SUMMARY.search(result.stdout)
            if result.returncode == 0 or rejected is None or int(rejected[2]) == 0 or "postcondition not satisfied" not in result.stdout:
                raise RuntimeError(f"{name}: mutant did not fail its intended postcondition")
            controls[name] = {"returncode": result.returncode, "summary": rejected[0], "failure_kind": "postcondition",
                              "source_sha256": runner.sha256(mutant.encode()), "output_sha256": runner.sha256(result.stdout.encode())}
        record = {
            "schema": "pg-trickle-join-plan-verification-v1", "source": "verification/join_plan.rs",
            "source_sha256": runner.sha256(source.encode()), "verus_image": runner.IMAGE,
            "compiler_identity": compiler, "solver_identity": solver, "obligations": OBLIGATIONS,
            "production_inputs": {path: runner.sha256((runner.ROOT / path).read_bytes()) for path in PRODUCTION_INPUTS},
            "baseline": {"returncode": baseline.returncode, "summary": summary[0], "output_sha256": runner.sha256(baseline.stdout.encode())},
            "semantic_mutation": controls["mixed-snapshot-admission"], "semantic_mutations": controls,
        }
        (EVIDENCE / "join-plan-verification.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        print(f"Join kernel verified; all five executable mutants rejected. Evidence: {EVIDENCE.relative_to(runner.ROOT)}")
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract-only", action="store_true")
    args = parser.parse_args()
    try:
        check_contract() if args.contract_only else run_checks()
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"join verification failed closed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
