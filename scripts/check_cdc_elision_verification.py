#!/usr/bin/env python3
"""Check the production CDC image policy with the pinned LSN proof runner."""

import argparse
import json
import subprocess
import sys

import check_lsn_verification as runner

SOURCE = runner.ROOT / "verification/cdc_elision.rs"
EVIDENCE = runner.ROOT / ".p2p/work/issue-1221-cdc-observations/evidence"
OBLIGATIONS = ["admitted_image_type", "keep_change", "image_action"]
PRODUCTION_INPUTS = ["src/lib.rs", "src/cdc/triggers.rs", "src/cdc/mod.rs", "src/cdc/rebuild.rs", "src/dvm/mod.rs", "src/dvm/diff.rs", "src/dvm/operators/scan.rs", "src/dvm/operators/project.rs", "src/refresh/codegen.rs", "Cargo.toml", "Cargo.lock"]
MUTATIONS = {
    "lost-image-change": ("    !admitted || !image_equal\n", "    false\n"),
    "unknown-type-elision": ("    !admitted || !image_equal\n", "    admitted && !image_equal\n"),
    "lost-positive-image": ("if weight > 0 { ImageAction::Insert }", "if weight > 0 { ImageAction::Skip }"),
    "reversed-negative-image": ("else if weight < 0 { ImageAction::Delete }", "else if weight < 0 { ImageAction::Insert }"),
}


def mutate(source: str, name: str) -> str:
    before, after = MUTATIONS[name]
    if source.count(before) != 1:
        raise ValueError("expected exactly one executable CDC keep decision")
    return source.replace(before, after, 1)


def check_contract() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    for name in MUTATIONS:
        mutate(source, name)
    if '#[path = "../verification/cdc_elision.rs"]' not in (runner.ROOT / "src/lib.rs").read_text():
        raise ValueError("production does not import the checked CDC policy")
    for path in ["src/cdc/triggers.rs"]:
        if "crate::cdc_elision::changed_column_sql(" not in (runner.ROOT / path).read_text():
            raise ValueError(f"{path} bypasses the checked CDC policy")
    if "crate::cdc_elision::keyed_weight_sql(" not in (runner.ROOT / "src/refresh/codegen.rs").read_text():
        raise ValueError("keyed MERGE does not execute the checked image action adapter")


def run_checks() -> None:
    check_contract()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    temporary = runner.ROOT / ".p2p/tmp/cdc-elision-mutant.rs"
    temporary.parent.mkdir(parents=True, exist_ok=True)
    source = SOURCE.read_text(encoding="utf-8")
    try:
        compiler = runner.run_tool_identity("verus", ["--version"], EVIDENCE / "cdc-verus-version.log")
        solver = runner.run_tool_identity("z3", ["--version"], EVIDENCE / "cdc-z3-version.log")
        baseline = runner.run_verus("verification/cdc_elision.rs", EVIDENCE / "cdc-verification-baseline.log")
        summary = runner.VERIFICATION_SUMMARY.search(baseline.stdout)
        if baseline.returncode or summary is None or int(summary[1]) < len(OBLIGATIONS) or int(summary[2]) != 0:
            raise RuntimeError("CDC policy baseline failed or omitted required obligations")
        controls = {}
        for name in MUTATIONS:
            mutant = mutate(source, name)
            temporary.write_text(mutant, encoding="utf-8")
            result = runner.run_verus(".p2p/tmp/cdc-elision-mutant.rs", EVIDENCE / f"cdc-mutant-{name}.log")
            rejected = runner.VERIFICATION_SUMMARY.search(result.stdout)
            if result.returncode == 0 or rejected is None or int(rejected[2]) == 0 or "postcondition not satisfied" not in result.stdout:
                raise RuntimeError(f"{name}: executable mutant did not fail its postcondition")
            controls[name] = {"returncode": result.returncode, "summary": rejected[0], "failure_kind": "postcondition",
                              "source_sha256": runner.sha256(mutant.encode()), "output_sha256": runner.sha256(result.stdout.encode())}
        record = {
            "schema": "pg-trickle-cdc-elision-verification-v1", "source": "verification/cdc_elision.rs",
            "source_sha256": runner.sha256(source.encode()), "verus_image": runner.IMAGE,
            "compiler_identity": compiler, "solver_identity": solver, "obligations": OBLIGATIONS,
            "production_inputs": {path: runner.sha256((runner.ROOT / path).read_bytes()) for path in PRODUCTION_INPUTS},
            "baseline": {"returncode": baseline.returncode, "summary": summary[0], "output_sha256": runner.sha256(baseline.stdout.encode())},
            "semantic_mutation": controls["lost-image-change"], "semantic_mutations": controls,
        }
        (EVIDENCE / "cdc-elision-verification.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        print(f"CDC policy verified; all four executable mutants rejected. Evidence: {EVIDENCE.relative_to(runner.ROOT)}")
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
        print(f"CDC verification failed closed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
