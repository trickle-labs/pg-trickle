"""Negative controls for the checked-LSN candidate binding helper."""

from __future__ import annotations

import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

sys_path = str(Path(__file__).resolve().parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

import check_lsn_candidate_binding as binding


class CandidateBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="lsn-binding-")
        self.root = Path(self.temp.name)
        self.package = self.root / "package"
        library = self.package / "usr/lib/postgresql/18/lib/pg_trickle.so"
        library.parent.mkdir(parents=True)
        library.write_bytes(b"candidate shared library")
        self.library_digest = hashlib.sha256(library.read_bytes()).hexdigest()
        self.source = binding.ROOT / "verification/lsn.rs"
        self.proof = {
            "schema": binding.EXPECTED_SCHEMA,
            "source_sha256": binding.digest(self.source),
            "verus_image": (
                "ghcr.io/verus-lang/verus:0.2025.06.23.2e59154"
                "@sha256:c4d0471379b23c3c6f52e3d7226c7dad28f488c4288e14c623002bd279a745e0"
            ),
            "compiler_identity": "Verus fixture identity",
            "solver_identity": "Z3 fixture identity",
            "obligations": sorted(binding.REQUIRED_OBLIGATIONS),
            "baseline": {"returncode": 0},
            "semantic_mutation": {"returncode": 1},
        }
        self.installation = [
            {
                "server": {"container_id": "fixture"},
                "candidate_files": [
                    {
                        "path": "usr/lib/postgresql/18/lib/pg_trickle.so",
                        "sha256": self.library_digest,
                    }
                ],
                "installed_files": [
                    {
                        "path": "usr/lib/postgresql/18/lib/pg_trickle.so",
                        "sha256": self.library_digest,
                    }
                ],
            }
        ]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_lifecycle_binding_requires_every_semantic_control_and_callers(self) -> None:
        self.source = binding.ROOT / "verification/aggregate_lifecycle.rs"
        contract = binding.PROOF_CONTRACTS["aggregate_lifecycle"]
        names = ["unknown-not-retracted", "missing-retraction", "retained-empty-group",
                 "missing-global-empty-result", "non-null-empty-sum"]
        self.proof.update({
            "schema": contract["schema"], "source_sha256": binding.digest(self.source),
            "obligations": sorted(contract["obligations"]),
            "production_inputs": {path: binding.digest(binding.ROOT / path) for path in contract["inputs"]},
            "semantic_mutations": {name: {"returncode": 1, "failure_kind": "postcondition",
                                           "summary": "verification results:: 5 verified, 1 errors"}
                                   for name in names},
        })
        arguments = dict(proof=self.proof, source=self.source, package_dir=self.package,
                         installation=self.installation, runtime_cases=binding.LIFECYCLE_CASES,
                         candidate_commit="fixture-commit", proof_kind="aggregate_lifecycle")
        self.assertEqual(binding.validate_binding(**arguments)["schema"],
                         "pg-trickle-aggregate-lifecycle-candidate-binding-v1")
        for name in names:
            control = self.proof["semantic_mutations"].pop(name)
            with self.assertRaisesRegex(ValueError, "omits a required executable mutant"):
                binding.validate_binding(**arguments)
            self.proof["semantic_mutations"][name] = {**control, "failure_kind": "compile-error"}
            with self.assertRaisesRegex(ValueError, "intended postcondition"):
                binding.validate_binding(**arguments)
            self.proof["semantic_mutations"][name] = control
        self.proof["production_inputs"]["src/dvm/operators/filter.rs"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "production input differs"):
            binding.validate_binding(**arguments)

    def test_receipt_binding_checks_production_inputs_and_installed_bytes(self) -> None:
        self.source = binding.ROOT / "verification/receipt.rs"
        self.proof.update({
            "schema": "pg-trickle-receipt-verification-v1",
            "source_sha256": binding.digest(self.source),
            "obligations": ["receipt_ack_row_limit"],
            "production_inputs": {
                path: binding.digest(binding.ROOT / path)
                for path in ["src/lib.rs", "src/wal_decoder.rs", "Cargo.toml", "Cargo.lock"]
            },
        })
        arguments = dict(
            proof=self.proof, source=self.source, package_dir=self.package,
            installation=self.installation, runtime_cases=binding.RECEIPT_CASES,
            candidate_commit="fixture-commit", proof_kind="receipt",
        )
        self.assertEqual(binding.validate_binding(**arguments)["schema"],
                         "pg-trickle-receipt-candidate-binding-v1")
        self.proof["production_inputs"]["src/wal_decoder.rs"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "production input differs"):
            binding.validate_binding(**arguments)

    def test_lsn_candidate_binding_accepts_matching_source_package_and_install(self) -> None:
        record = binding.validate_binding(
            proof=self.proof,
            source=self.source,
            package_dir=self.package,
            installation=self.installation,
            runtime_cases=binding.EXPECTED_CASES,
            candidate_commit="fixture-commit",
        )
        self.assertEqual(record["candidate_package_library_sha256"], self.library_digest)
        self.assertEqual(record["installed_observations"][0]["installed_sha256"], self.library_digest)

    def test_lsn_candidate_binding_rejects_source_mismatch(self) -> None:
        self.proof["source_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "source digest does not match"):
            binding.validate_binding(
                proof=self.proof,
                source=self.source,
                package_dir=self.package,
                installation=self.installation,
                runtime_cases=binding.EXPECTED_CASES,
                candidate_commit="fixture-commit",
            )

    def test_lsn_candidate_binding_rejects_installed_library_mismatch(self) -> None:
        self.installation[0]["installed_files"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "installed candidate library digest differs"):
            binding.validate_binding(
                proof=self.proof,
                source=self.source,
                package_dir=self.package,
                installation=self.installation,
                runtime_cases=binding.EXPECTED_CASES,
                candidate_commit="fixture-commit",
            )

    def test_lsn_candidate_binding_rejects_incomplete_runtime_cases(self) -> None:
        with self.assertRaisesRegex(ValueError, "all required LSN E2E cases"):
            binding.validate_binding(
                proof=self.proof,
                source=self.source,
                package_dir=self.package,
                installation=self.installation,
                runtime_cases=binding.EXPECTED_CASES[:-1],
                candidate_commit="fixture-commit",
            )


if __name__ == "__main__":
    unittest.main()
