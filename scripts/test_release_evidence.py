import tarfile
import tempfile
import unittest
from pathlib import Path

from scripts.release_evidence import (
    archive_payload_manifest,
    canonical_digest,
    validate_attempts,
    validate_case_evidence,
    validate_installation,
)
from scripts.run_release_suite import machine_case_attempts


CASE = "e2e_example_tests::test_required_case"


class ReleaseEvidenceTests(unittest.TestCase):
    def test_machine_case_parser_preserves_full_test_identity(self) -> None:
        case = "e2e_sensitivity_baseline_tests::test_oracle_detects_schema_mismatch"
        self.assertEqual(
            machine_case_attempts(
                "\n".join(
                    [
                        '{"type":"test","event":"started","name":"pg_trickle::e2e_sensitivity_baseline_tests$test_oracle_detects_schema_mismatch"}',
                        '{"type":"test","event":"ok","name":"pg_trickle::e2e_sensitivity_baseline_tests$test_oracle_detects_schema_mismatch"}',
                    ]
                ),
                [case],
            ),
            [[{"id": case, "status": "passed"}]],
        )

    def test_required_case_omission_is_rejected(self) -> None:
        result = {
            "suite_id": "example",
            "selected_cases": [CASE],
            "observed_cases": [],
        }
        with self.assertRaisesRegex(ValueError, "observed case"):
            validate_case_evidence(result, {"required_cases": [CASE]})

    def test_failed_retry_cannot_be_erased(self) -> None:
        result = {
            "suite_id": "example",
            "status": "passed",
            "retry_count": 1,
            "attempts": [
                {"attempt": 1, "status": "failed", "cases": []},
                {"attempt": 2, "status": "passed", "cases": []},
            ],
        }
        with self.assertRaisesRegex(ValueError, "non-passing attempt"):
            validate_attempts(result, {"required": True})

    def test_installed_payload_mismatch_is_rejected(self) -> None:
        files = [{"path": "usr/lib/postgresql/18/lib/pg_trickle.so", "bytes": 3, "sha256": "abc"}]
        result = {
            "suite_id": "example",
            "candidate_commit": "a" * 40,
            "artifact_id": "linux-amd64",
            "artifact_digest": "artifact",
            "platform": "linux-amd64",
            "build_kind": "exact-release",
            "feature_scope": "default-release",
            "candidate_payload": {"files": files, "sha256": canonical_digest(files)},
            "server_configuration": [{}],
            "installation_observations": [{
                "candidate_commit": "a" * 40,
                "artifact_id": "linux-amd64",
                "artifact_digest": "artifact",
                "platform": "linux-amd64",
                "build_kind": "exact-release",
                "feature_scope": "default-release",
                "payload_digest": canonical_digest(files),
                "installed_payload_digest": canonical_digest([{"path": files[0]["path"], "bytes": 4, "sha256": "def"}]),
                "candidate_files": files,
                "installed_files": [{"path": files[0]["path"], "bytes": 4, "sha256": "def"}],
                "server": {"fresh_postmaster": True, "server_version": "18.3"},
            }],
        }
        with self.assertRaisesRegex(ValueError, "installed payload"):
            validate_installation(
                result,
                {"runtime": True},
                {"sha256": "artifact", "payload_manifest": result["candidate_payload"]},
                {},
            )

    def test_archive_payload_manifest_is_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "pg_trickle-0.108.0-pg18-linux-amd64"
            (root / "lib").mkdir(parents=True)
            (root / "extension").mkdir()
            (root / "lib" / "pg_trickle.so").write_bytes(b"binary")
            (root / "extension" / "pg_trickle.control").write_bytes(b"control")
            (root / "release-candidate.json").write_text("{}", encoding="utf-8")
            archive_path = Path(temporary) / "candidate.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                archive.add(root, arcname=root.name)

            manifest = archive_payload_manifest(archive_path)
            self.assertEqual(
                [file["path"] for file in manifest["files"]],
                [
                    "usr/lib/postgresql/18/lib/pg_trickle.so",
                    "usr/share/postgresql/18/extension/pg_trickle.control",
                ],
            )


if __name__ == "__main__":
    unittest.main()
