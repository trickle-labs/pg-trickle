#!/usr/bin/env python3
"""Verify production scalar identity framing and checked tuple capacity."""

import argparse
import json
import re
import subprocess
import sys

import check_lsn_verification as runner

SOURCE = runner.ROOT / "verification/identity_frame.rs"
EVIDENCE = runner.ROOT / ".p2p/work/issue-1224-identity-framing/evidence"
OBLIGATIONS = ["fixed_scalar_width", "checked_frame_size", "validate_scalar_frame", "encode_scalar_frame", "unique_scalar_decoding"]
PRODUCTION_INPUTS = [
    "src/lib.rs", "src/dvm/row_id_v2.rs", "src/dvm/row_id.rs", "src/hash.rs", "src/refresh/codegen.rs",
    "src/dvm/operators/join_common.rs", "src/refresh/merge/mod.rs", "src/refresh/merge/insert.rs",
    "tests/row_id_v2_vectors.rs", "tests/fixtures/row_id_v2_vectors.json", "tests/e2e_row_id_v2_tests.rs",
    "Cargo.toml", "Cargo.lock",
]
MUTATIONS = {
    "lost-terminator": ("    out.push(255);", "    out.push(254);"),
    "trailing-input": ("input.len() == 9 + width", "input.len() >= 9 + width"),
    "invalid-bool-payload": ("(input[6] != 1 || input[8] <= 1)", "true"),
    "lost-domain": (" || input[1] < 1 || input[1] > 8", ""),
    "lost-size-payload": ("let size = 7 + 2 * field_count + payload_bytes;", "let size = 7 + 2 * field_count;"),
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
            raise ValueError(f"missing framing obligation: {name}")
    for name in MUTATIONS:
        mutate(source, name)
    if '#[path = "../verification/identity_frame.rs"]' not in (runner.ROOT / "src/lib.rs").read_text():
        raise ValueError("production does not import the checked framing kernel")
    caller = (runner.ROOT / "src/dvm/row_id_v2.rs").read_text()
    for name in ["fixed_scalar_width", "encode_scalar_frame", "validate_scalar_frame", "checked_frame_size"]:
        if f"crate::identity_frame::{name}(" not in caller:
            raise ValueError(f"production bypasses {name}")
    if "encode_tuple_with_version(identity_version, domain, &borrowed)" not in caller:
        raise ValueError("PostgreSQL records bypass the shared tuple framer")
    for declaration in ["pub const MAX_TUPLE_FIELDS: usize = 1024;",
                        "pub const MAX_ENCODED_IDENTITY_BYTES: usize = 1024 * 1024;"]:
        if declaration not in caller:
            raise ValueError("ordinary tuple limits differ from checked capacity")


def run_checks() -> None:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    record = EVIDENCE / "identity-frame-verification.json"
    record.unlink(missing_ok=True)
    check_contract()
    temporary = runner.ROOT / ".p2p/tmp/identity-frame-mutant.rs"
    temporary.parent.mkdir(parents=True, exist_ok=True)
    source = SOURCE.read_text(encoding="utf-8")
    try:
        compiler = runner.run_tool_identity("verus", ["--version"], EVIDENCE / "identity-verus-version.log")
        solver = runner.run_tool_identity("z3", ["--version"], EVIDENCE / "identity-z3-version.log")
        baseline = runner.run_verus("verification/identity_frame.rs", EVIDENCE / "identity-verification-baseline.log")
        summary = runner.VERIFICATION_SUMMARY.search(baseline.stdout)
        if baseline.returncode or summary is None or int(summary[1]) < 6 or int(summary[2]) != 0:
            raise RuntimeError("identity baseline failed or omitted required obligations")
        controls = {}
        for name in MUTATIONS:
            mutant = mutate(source, name)
            temporary.write_text(mutant, encoding="utf-8")
            result = runner.run_verus(".p2p/tmp/identity-frame-mutant.rs", EVIDENCE / f"identity-mutant-{name}.log")
            rejected = runner.VERIFICATION_SUMMARY.search(result.stdout)
            if result.returncode == 0 or rejected is None or int(rejected[2]) == 0 or "postcondition not satisfied" not in result.stdout:
                raise RuntimeError(f"{name}: mutant did not fail its intended postcondition")
            controls[name] = {"returncode": result.returncode, "summary": rejected[0], "failure_kind": "postcondition",
                              "source_sha256": runner.sha256(mutant.encode()), "output_sha256": runner.sha256(result.stdout.encode())}
        record.write_text(json.dumps({
            "schema": "pg-trickle-identity-frame-verification-v1", "source": "verification/identity_frame.rs",
            "source_sha256": runner.sha256(source.encode()), "verus_image": runner.IMAGE,
            "compiler_identity": compiler, "solver_identity": solver, "obligations": OBLIGATIONS,
            "production_inputs": {path: runner.sha256((runner.ROOT / path).read_bytes()) for path in PRODUCTION_INPUTS},
            "baseline": {"returncode": baseline.returncode, "summary": summary[0], "output_sha256": runner.sha256(baseline.stdout.encode())},
            "semantic_mutation": controls["lost-terminator"], "semantic_mutations": controls,
        }, indent=2) + "\n", encoding="utf-8")
        print(f"Scalar framing and tuple capacity verified; five intended mutants rejected. Evidence: {record.relative_to(runner.ROOT)}")
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
        print(f"identity verification failed closed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
