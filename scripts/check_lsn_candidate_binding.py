#!/usr/bin/env python3
"""Bind the verified LSN source to a packaged and installed candidate library."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_SCHEMA = "pg-trickle-lsn-verification-v1"
EXPECTED_CASES = [
    "e2e_lsn_contract_tests::test_lsn_integrated_callers_preserve_valid_and_invalid_frontiers",
    "e2e_lsn_contract_tests::test_lsn_scheduler_preserves_malformed_frontier_and_recovers",
    "e2e_lsn_contract_tests::test_lsn_scheduler_cdc_holdback_recovers_after_long_writer",
    "e2e_lsn_contract_tests::test_lsn_wal_transition_rejects_malformed_and_preserves_results",
]
REQUIRED_OBLIGATIONS = {
    "lsn_parse_value_and_bounds",
    "lsn_numeric_order",
    "lsn_format_parse_roundtrip",
}
RECEIPT_CASES = [
    "e2e_wal_receipt_recovery_tests::test_wal_receipt_small_limits_preserve_complete_transactions",
]
LIFECYCLE_CASES = [
    "e2e_having_transition_tests::test_having_all_nine_truth_transitions_match_postgresql",
    "e2e_having_transition_tests::test_having_grouped_and_global_empty_lifecycle_match_postgresql",
]
CDC_ELISION_CASES = [
    "e2e_shared_buffer_tests::test_shared_numeric_images_preserve_scale_for_both_trigger_modes_and_orders",
]
PROOF_CONTRACTS = {
    "lsn": {
        "schema": EXPECTED_SCHEMA, "obligations": REQUIRED_OBLIGATIONS,
        "cases": EXPECTED_CASES, "inputs": [],
    },
    "receipt": {
        "schema": "pg-trickle-receipt-verification-v1", "obligations": {"receipt_ack_row_limit"},
        "cases": RECEIPT_CASES, "inputs": ["src/lib.rs", "src/wal_decoder.rs", "Cargo.toml", "Cargo.lock"],
    },
    "aggregate_lifecycle": {
        "schema": "pg-trickle-aggregate-lifecycle-verification-v1",
        "obligations": {"visibility_actions", "group_present", "group_action", "aggregate_is_null"},
        "cases": LIFECYCLE_CASES,
        "inputs": ["src/lib.rs", "src/dvm/operators/filter.rs", "src/dvm/operators/aggregate.rs", "Cargo.toml", "Cargo.lock"],
        "mutants": {"unknown-not-retracted", "missing-retraction", "retained-empty-group", "missing-global-empty-result", "non-null-empty-sum"},
    },
    "cdc_elision": {
        "schema": "pg-trickle-cdc-elision-verification-v1",
        "obligations": {"admitted_image_type", "keep_change"},
        "cases": CDC_ELISION_CASES,
        "inputs": ["src/lib.rs", "src/cdc/triggers.rs", "src/cdc/mod.rs", "Cargo.toml", "Cargo.lock"],
        "mutants": {"lost-image-change", "unknown-type-elision"},
    },
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def proof_source_digest(proof: dict[str, Any], source: Path, proof_kind: str = "lsn") -> str:
    require(proof_kind in PROOF_CONTRACTS, "unknown proof kind")
    contract = PROOF_CONTRACTS[proof_kind]
    require(proof.get("schema") == contract["schema"], "proof record has an unknown schema")
    require(proof.get("baseline", {}).get("returncode") == 0, "LSN proof baseline did not pass")
    require(proof.get("semantic_mutation", {}).get("returncode", 0) != 0,
            "LSN semantic mutation was not rejected")
    require(contract["obligations"].issubset(set(proof.get("obligations", []))),
            "proof record omits a required LSN obligation")
    require(bool(proof.get("compiler_identity")), "proof record omits the compiler identity")
    require(bool(proof.get("solver_identity")), "proof record omits the solver identity")
    if required := contract.get("mutants"):
        controls = proof.get("semantic_mutations", {})
        require(isinstance(controls, dict) and required.issubset(controls),
                f"{proof_kind} proof omits a required executable mutant")
        for name in required:
            control = controls[name]
            require(isinstance(control, dict) and control.get("returncode", 0) != 0
                    and control.get("failure_kind") == "postcondition"
                    and re.search(r"verification results::\s+\d+ verified,\s+[1-9]\d* errors",
                                  control.get("summary", "")) is not None,
                    f"{proof_kind} mutant did not fail its intended postcondition: {name}")
    for path in contract["inputs"]:
        require(proof.get("production_inputs", {}).get(path) == digest(ROOT / path),
                f"{proof_kind} production input differs from verified candidate: {path}")
    return digest(source)


def validate_binding(
    *,
    proof: dict[str, Any],
    source: Path,
    package_dir: Path,
    installation: list[dict[str, Any]],
    runtime_cases: list[str],
    candidate_commit: str,
    proof_kind: str = "lsn",
) -> dict[str, Any]:
    source_digest = proof_source_digest(proof, source, proof_kind)
    require(
        proof.get("source_sha256") == source_digest,
        "verified source digest does not match the candidate executable source",
    )
    require(
        proof.get("verus_image", "").endswith(
            "@sha256:c4d0471379b23c3c6f52e3d7226c7dad28f488c4288e14c623002bd279a745e0"
        ),
        "proof record does not use the pinned Verus image",
    )
    expected_cases = PROOF_CONTRACTS[proof_kind]["cases"]
    require(runtime_cases == expected_cases,
            f"runtime evidence must name all required {proof_kind.upper()} E2E cases")
    package_libraries = sorted((package_dir / "usr/lib/postgresql/18/lib").glob("pg_trickle.so"))
    require(len(package_libraries) == 1, "candidate package must contain exactly one pg_trickle.so")
    package_lib = package_libraries[0]
    package_digest = digest(package_lib)
    install_observations = [
        observation
        for observation in installation
        if any(
            item.get("path") == "usr/lib/postgresql/18/lib/pg_trickle.so"
            for item in observation.get("candidate_files", [])
        )
    ]
    require(bool(install_observations), "runtime attestation has no candidate pg_trickle.so entry")
    observed: list[dict[str, str]] = []
    for observation in install_observations:
        require(bool(candidate_commit) and observation.get("candidate_commit") == candidate_commit,
                "runtime attestation candidate commit is missing or differs from the requested candidate")
        candidate = next(
            item for item in observation["candidate_files"]
            if item.get("path") == "usr/lib/postgresql/18/lib/pg_trickle.so"
        )
        installed = next(
            (
                item for item in observation.get("installed_files", [])
                if item.get("path") == "usr/lib/postgresql/18/lib/pg_trickle.so"
            ),
            None,
        )
        require(installed is not None, "runtime attestation omits installed pg_trickle.so")
        require(candidate.get("sha256") == package_digest, "attested package digest differs from package bytes")
        require(installed.get("sha256") == package_digest, "installed candidate library digest differs from package")
        observed.append(
            {
                "container_id": str(observation.get("server", {}).get("container_id", "unknown")),
                "candidate_sha256": str(candidate["sha256"]),
                "installed_sha256": str(installed["sha256"]),
            }
        )
    return {
        "schema": f"pg-trickle-{proof_kind.replace('_', '-')}-candidate-binding-v1",
        "candidate_commit": candidate_commit,
        "provenance_boundary": "Build origin and successful named test execution are supplied by the trusted build/test runner; this record checks source hashes and package/install bytes.",
        "workflow": os.environ.get("GITHUB_WORKFLOW", "local"),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "source": source.relative_to(ROOT).as_posix(),
        "source_sha256": source_digest,
        "proof_record_schema": proof["schema"],
        "verus_image": proof["verus_image"],
        "compiler_identity": proof["compiler_identity"],
        "solver_identity": proof["solver_identity"],
        "candidate_package_library_sha256": package_digest,
        "installed_observations": observed,
        "runtime_cases": runtime_cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof-record", type=Path, required=True)
    parser.add_argument("--proof-kind", choices=list(PROOF_CONTRACTS), default="lsn")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--package-dir", type=Path, required=True)
    parser.add_argument("--installation", type=Path, required=True)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--runtime-case", action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        proof = json.loads(args.proof_record.read_text(encoding="utf-8"))
        installation = json.loads(args.installation.read_text(encoding="utf-8"))
        require(isinstance(installation, list), "installation attestation must be a JSON list")
        record = validate_binding(
            proof=proof,
            source=(args.source or ROOT / f"verification/{args.proof_kind}.rs").resolve(),
            package_dir=args.package_dir.resolve(),
            installation=installation,
            runtime_cases=args.runtime_case,
            candidate_commit=args.candidate_commit,
            proof_kind=args.proof_kind,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        print(f"{args.proof_kind.upper()} source/package/installation binding passed: {args.output}")
        return 0
    except (OSError, ValueError, KeyError, StopIteration, json.JSONDecodeError) as error:
        print(f"LSN candidate binding failed closed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
