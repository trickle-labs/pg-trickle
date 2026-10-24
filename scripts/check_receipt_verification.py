#!/usr/bin/env python3
"""Check the production receipt limit with the existing pinned LSN proof runner."""

import argparse
import json
import subprocess
import sys

import check_lsn_verification as runner

SOURCE = runner.ROOT / "verification/receipt.rs"
EVIDENCE = runner.ROOT / ".p2p/work/issue-1222-receipt-prefix/evidence"
OBLIGATIONS = ["receipt_ack_row_limit", "may_skip_source_free_row"]
MUTATIONS = {
    "off-by-one": ("Some(message_count as i32)", "Some((message_count + 1) as i32)"),
    "skip-source-row": ("    !source_change && recognized_table && ordinary_dml\n", "    recognized_table && ordinary_dml\n"),
    "skip-unrecognized-table": ("    !source_change && recognized_table && ordinary_dml\n", "    !source_change && ordinary_dml\n"),
    "skip-other-action": ("    !source_change && recognized_table && ordinary_dml\n", "    !source_change && recognized_table\n"),
}


def mutate(source: str, name: str = "off-by-one") -> str:
    original, replacement = MUTATIONS[name]
    if source.count(original) != 1:
        raise ValueError("expected exactly one executable acknowledgement limit")
    return source.replace(original, replacement, 1)


def check_contract() -> None:
    for name in MUTATIONS:
        mutate(SOURCE.read_text(encoding="utf-8"), name)
    if '#[path = "../verification/receipt.rs"]' not in (runner.ROOT / "src/lib.rs").read_text():
        raise ValueError("production does not import the verified receipt implementation")
    if "crate::receipt::receipt_ack_row_limit(expected.len())" not in (
        runner.ROOT / "src/wal_decoder.rs"
    ).read_text():
        raise ValueError("production acknowledgement does not execute the verified limit")
    if "crate::receipt::may_skip_source_free_row(" not in (runner.ROOT / "src/wal_decoder.rs").read_text():
        raise ValueError("production bookkeeping classification bypasses the verified decision")


def run_checks() -> None:
    check_contract()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    mutant = runner.ROOT / ".p2p/tmp/receipt-semantic-mutant.rs"
    mutant.parent.mkdir(parents=True, exist_ok=True)
    source = SOURCE.read_text(encoding="utf-8")
    try:
        compiler = runner.run_tool_identity("verus", ["--version"], EVIDENCE / "receipt-verus-version.log")
        solver = runner.run_tool_identity("z3", ["--version"], EVIDENCE / "receipt-z3-version.log")
        baseline = runner.run_verus("verification/receipt.rs", EVIDENCE / "receipt-verification-baseline.log")
        summary = runner.VERIFICATION_SUMMARY.search(baseline.stdout)
        if baseline.returncode or summary is None or int(summary[1]) < len(OBLIGATIONS) or int(summary[2]) != 0:
            raise RuntimeError("receipt baseline failed or omitted the intended obligation")
        controls = {}
        for name in MUTATIONS:
            mutant_source = mutate(source, name)
            mutant.write_text(mutant_source, encoding="utf-8")
            result = runner.run_verus(".p2p/tmp/receipt-semantic-mutant.rs", EVIDENCE / f"receipt-mutant-{name}.log")
            rejected = runner.VERIFICATION_SUMMARY.search(result.stdout)
            if result.returncode == 0 or rejected is None or int(rejected[2]) == 0 or "postcondition not satisfied" not in result.stdout:
                raise RuntimeError(f"{name}: receipt executable mutant did not fail its intended postcondition")
            controls[name] = {"returncode": result.returncode, "summary": rejected[0], "failure_kind": "postcondition",
                              "source_sha256": runner.sha256(mutant_source.encode()), "output_sha256": runner.sha256(result.stdout.encode())}
        record = {
            "schema": "pg-trickle-receipt-verification-v1",
            "source": "verification/receipt.rs",
            "source_sha256": runner.sha256(source.encode()),
            "verus_image": runner.IMAGE,
            "compiler_identity": compiler,
            "solver_identity": solver,
            "obligations": OBLIGATIONS,
            "production_inputs": {
                path: runner.sha256((runner.ROOT / path).read_bytes())
                for path in ["src/lib.rs", "src/wal_decoder.rs", "Cargo.toml", "Cargo.lock"]
            },
            "baseline": {"returncode": baseline.returncode, "summary": summary[0],
                         "output_sha256": runner.sha256(baseline.stdout.encode())},
            "semantic_mutation": controls["off-by-one"], "semantic_mutations": controls,
        }
        (EVIDENCE / "receipt-verification.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        print("Receipt limit and bookkeeping gate verified; all four executable mutants rejected. "
              f"Evidence: {EVIDENCE.relative_to(runner.ROOT)}")
    finally:
        mutant.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract-only", action="store_true")
    args = parser.parse_args()
    try:
        check_contract() if args.contract_only else run_checks()
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"receipt verification failed closed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
