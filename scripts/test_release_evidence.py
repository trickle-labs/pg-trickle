import hashlib
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

from scripts.release_evidence import (
    archive_payload_manifest,
    canonical_digest,
    validate_attempts,
    validate_case_evidence,
    validate_case_log,
    validate_installation,
)
from scripts.stage_release_assets import stage_release_assets
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

    def test_machine_case_parser_keeps_retried_failures(self) -> None:
        name = "pg_trickle::e2e_example_tests$test_required_case"
        output = "\n".join(
            f'{{"type":"test","event":"{event}","name":"{name}#{attempt}"}}'
            for event, attempt in (("failed", 1), ("ok", 2))
        )
        self.assertEqual(
            machine_case_attempts(output, [CASE]),
            [[{"id": CASE, "status": "failed"}], [{"id": CASE, "status": "passed"}]],
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

    def test_failed_machine_event_cannot_be_dropped_from_result(self) -> None:
        name = "pg_trickle::e2e_example_tests$test_required_case"
        output = "\n".join(
            f'{{"type":"test","event":"{event}","name":"{name}"}}'
            for event in ("failed", "ok")
        )
        result = {
            "suite_id": "example",
            "attempts": [{"cases": [{"id": CASE, "status": "passed"}]}],
            "observed_cases": [{"id": CASE, "status": "passed"}],
        }
        with self.assertRaisesRegex(ValueError, "machine events"):
            validate_case_log(result, {"required_cases": [CASE]}, output)

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

    def test_structured_cli_rejects_manually_declared_suite_pass(self) -> None:
        root = Path(__file__).resolve().parents[1]
        qualification = root / "tests/release/v0.108.0-qualification.json"
        with tempfile.TemporaryDirectory() as temporary:
            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/release_evidence.py",
                    "--output",
                    str(Path(temporary) / "RELEASE-EVIDENCE.json"),
                    "--version",
                    "0.108.0",
                    "--candidate-commit",
                    "a" * 40,
                    "--qualification",
                    str(qualification),
                    "--suite",
                    "sensitivity-baseline=passed",
                ],
                cwd=root,
                text=True,
                capture_output=True,
                check=False,
            )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("manually declared suite results", completed.stderr)

    def test_release_runner_records_unavailable_runtime_diagnostic(self) -> None:
        root = Path(__file__).resolve().parents[1]
        qualification = json.loads(
            (root / "tests/release/v0.108.0-qualification.json").read_text(encoding="utf-8")
        )
        suite = next(item for item in qualification["required_suites"] if item["id"] == "sensitivity-baseline")
        diagnostic = "Docker daemon unavailable for qualification"
        suite["command_argv"] = [
            "bash", "-c", f"printf '%s\\n' '{diagnostic}'; exit 125",
        ]
        suite["command"] = " ".join(suite["command_argv"])
        candidate_commit = "a" * 40
        (root / "target").mkdir(exist_ok=True)

        with tempfile.TemporaryDirectory(prefix="pgt-unavailable-proof-", dir=root / "target") as temporary:
            directory = Path(temporary)
            package = directory / "package"
            (package / "lib").mkdir(parents=True)
            (package / "extension").mkdir()
            (package / "lib/pg_trickle.so").write_bytes(b"test library")
            (package / "extension/pg_trickle.control").write_text(
                "default_version = '0.108.0'\n", encoding="utf-8"
            )
            identity = {
                "candidate_commit": candidate_commit,
                "build_kind": qualification["build_kind"],
                "feature_scope": qualification["feature_scope"],
                "build_provenance": qualification["build_provenance"],
            }
            (package / "release-candidate.json").write_text(json.dumps(identity), encoding="utf-8")
            artifact = directory / "pg_trickle-0.108.0-pg18-linux-amd64.tar.gz"
            with tarfile.open(artifact, "w:gz") as archive:
                archive.add(package, arcname=package.name)
            contract_path = directory / "qualification.json"
            contract_path.write_text(json.dumps(qualification), encoding="utf-8")
            result_path = directory / "suite.json"
            log_path = directory / "suite.log"

            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/run_release_suite.py",
                    "--qualification",
                    str(contract_path),
                    "--suite",
                    "sensitivity-baseline",
                    "--artifact",
                    str(artifact),
                    "--candidate-commit",
                    candidate_commit,
                    "--log",
                    str(log_path),
                    "--result",
                    str(result_path),
                ],
                cwd=root,
                text=True,
                capture_output=True,
                check=False,
            )
            record = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertNotEqual(completed.returncode, 0)
            self.assertNotEqual(record["status"], "passed")
            self.assertTrue(all(case["status"] == "missing" for case in record["observed_cases"]))
            self.assertIn(diagnostic, log_path.read_text(encoding="utf-8"))

    def test_release_asset_staging_retains_and_verifies_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            evidence_dir = workspace / "qualification-logs"
            log_path = evidence_dir / "logs/release.log"
            measurement_path = evidence_dir / "raw/measurement.json"
            attachment_path = evidence_dir / "raw/measurement.svg"
            package_dir = workspace / "dist/release-linux-amd64"
            output_dir = workspace / "release-files"
            for path in (log_path, measurement_path, attachment_path, package_dir / "pg_trickle-pg18-linux-amd64.tar.gz"):
                path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_bytes(b"required suite passed\n")
            measurement_path.write_bytes(b'{"kind":"database-workloads"}\n')
            attachment_path.write_bytes(b"raw measurement graph\n")
            package = package_dir / "pg_trickle-pg18-linux-amd64.tar.gz"
            package.write_bytes(b"release package bytes")

            def reference(path: Path) -> dict[str, object]:
                data = path.read_bytes()
                return {
                    "path": path.relative_to(workspace).as_posix(),
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }

            manifest = {
                "status": "passed",
                "retained_logs": [reference(log_path)],
                "retained_measurements": [
                    {**reference(measurement_path), "attachments": [reference(attachment_path)]}
                ],
            }
            (evidence_dir / "RELEASE-EVIDENCE.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )

            stage_release_assets(evidence_dir, workspace / "dist", output_dir, workspace=workspace)

            self.assertEqual((output_dir / package.name).read_bytes(), package.read_bytes())
            with tarfile.open(output_dir / "qualification-evidence.tar.gz", "r:gz") as archive:
                names = set(archive.getnames())
                self.assertIn("qualification-logs/RELEASE-EVIDENCE.json", names)
                self.assertIn("qualification-logs/logs/release.log", names)
                self.assertIn("qualification-logs/raw/measurement.json", names)
                self.assertIn("qualification-logs/raw/measurement.svg", names)

            manifest["retained_logs"][0]["sha256"] = "0" * 64
            (evidence_dir / "RELEASE-EVIDENCE.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "mismatched size or digest"):
                stage_release_assets(
                    evidence_dir,
                    workspace / "dist",
                    workspace / "release-files-tampered",
                    workspace=workspace,
                )


if __name__ == "__main__":
    unittest.main()
