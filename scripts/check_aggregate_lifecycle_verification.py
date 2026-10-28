#!/usr/bin/env python3
"""Verify production lifecycle decisions and require their semantic controls to fail."""

import argparse
import json
import re
import subprocess
import sys

import check_lsn_verification as runner

SOURCE = runner.ROOT / "verification/aggregate_lifecycle.rs"
EVIDENCE = runner.ROOT / ".p2p/work/issue-1220-aggregate-lifecycle/evidence"
OBLIGATIONS = ["visibility_actions", "group_present", "group_action", "aggregate_is_null"]
PRODUCTION_INPUTS = ["src/lib.rs", "src/dvm/operators/filter.rs", "src/dvm/operators/aggregate.rs", "Cargo.toml", "Cargo.lock"]
MUTATIONS = {
    "unknown-not-retracted": (
        "retract: old_visible && (!new_visible || payload_changed),",
        "retract: old_visible && ((!new_exists || new_predicate == Some(false)) || payload_changed),",
    ),
    "missing-retraction": (
        "retract: old_visible && (!new_visible || payload_changed),", "retract: false,",
    ),
    "retained-empty-group": ("    global || has_rows\n", "    true\n"),
    "missing-global-empty-result": ("    global || has_rows\n", "    has_rows\n"),
    "non-null-empty-sum": ("    nullable && !has_arguments\n", "    false\n"),
}


def mutate(source: str, name: str) -> str:
    before, after = MUTATIONS[name]
    if source.count(before) != 1:
        raise ValueError(f"{name}: expected exactly one executable decision")
    return source.replace(before, after, 1)


def check_contract() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    for obligation in OBLIGATIONS:
        if not re.search(rf"pub fn {obligation}\b", source):
            raise ValueError(f"missing lifecycle obligation: {obligation}")
    for name in MUTATIONS:
        mutate(source, name)
    if '#[path = "../verification/aggregate_lifecycle.rs"]' not in (runner.ROOT / "src/lib.rs").read_text():
        raise ValueError("production does not import the checked lifecycle executable")
    for path, names in {
        "src/dvm/operators/filter.rs": ["having_admission_sql", "having_retraction_sql"],
        "src/dvm/operators/aggregate.rs": ["group_action_sql", "group_presence_sql", "aggregate_value_sql"],
    }.items():
        caller = (runner.ROOT / path).read_text()
        for name in names:
            if f"crate::aggregate_lifecycle::{name}(" not in caller:
                raise ValueError(f"{path} bypasses the checked {name} decision")


def run_checks() -> None:
    check_contract()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    temporary = runner.ROOT / ".p2p/tmp/aggregate-lifecycle-mutant.rs"
    temporary.parent.mkdir(parents=True, exist_ok=True)
    source = SOURCE.read_text(encoding="utf-8")
    try:
        compiler = runner.run_tool_identity("verus", ["--version"], EVIDENCE / "lifecycle-verus-version.log")
        solver = runner.run_tool_identity("z3", ["--version"], EVIDENCE / "lifecycle-z3-version.log")
        baseline = runner.run_verus("verification/aggregate_lifecycle.rs", EVIDENCE / "lifecycle-verification-baseline.log")
        summary = runner.VERIFICATION_SUMMARY.search(baseline.stdout)
        if baseline.returncode or summary is None or int(summary[1]) < len(OBLIGATIONS) or int(summary[2]) != 0:
            raise RuntimeError("lifecycle baseline failed or omitted the required obligations")
        controls = {}
        for name in MUTATIONS:
            mutant = mutate(source, name)
            temporary.write_text(mutant, encoding="utf-8")
            result = runner.run_verus(".p2p/tmp/aggregate-lifecycle-mutant.rs", EVIDENCE / f"lifecycle-mutant-{name}.log")
            rejected = runner.VERIFICATION_SUMMARY.search(result.stdout)
            if result.returncode == 0 or rejected is None or int(rejected[2]) == 0 or "postcondition not satisfied" not in result.stdout:
                raise RuntimeError(f"{name}: executable mutation was not rejected for its postcondition")
            controls[name] = {"returncode": result.returncode, "summary": rejected[0],
                              "failure_kind": "postcondition",
                              "source_sha256": runner.sha256(mutant.encode()),
                              "output_sha256": runner.sha256(result.stdout.encode())}
        record = {
            "schema": "pg-trickle-aggregate-lifecycle-verification-v1",
            "source": "verification/aggregate_lifecycle.rs", "source_sha256": runner.sha256(source.encode()),
            "verus_image": runner.IMAGE, "compiler_identity": compiler, "solver_identity": solver,
            "obligations": OBLIGATIONS,
            "production_inputs": {path: runner.sha256((runner.ROOT / path).read_bytes()) for path in PRODUCTION_INPUTS},
            "baseline": {"returncode": baseline.returncode, "summary": summary[0],
                         "output_sha256": runner.sha256(baseline.stdout.encode())},
            "semantic_mutation": controls["unknown-not-retracted"], "semantic_mutations": controls,
        }
        (EVIDENCE / "lifecycle-verification.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        print(f"Lifecycle decisions verified; {len(controls)} executable mutants rejected. Evidence: {EVIDENCE.relative_to(runner.ROOT)}")
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
        print(f"lifecycle verification failed closed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
