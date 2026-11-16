import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import live_rag_attestation as attestation


class AttestationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.archive = Path(self.tmp.name) / "candidate.tar.gz"
        self.commit = "a" * 40
        with tarfile.open(self.archive, "w:gz") as package:
            for name, data in {
                "release-candidate.json": json.dumps({"candidate_commit": self.commit, "build_kind": "exact-release", "feature_scope": "default-release"}).encode(),
                "lib/pg_trickle.so": b"library",
                "extension/pg_trickle.control": b"control",
                "extension/pg_trickle--0.109.0.sql": b"sql",
            }.items():
                member = tarfile.TarInfo("candidate/" + name)
                member.size = len(data)
                package.addfile(member, io.BytesIO(data))
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.files = attestation.archive_payload(self.archive, self.digest, self.commit)
        self.identity = {"system_identifier": "123", "postmaster_start": "2026-01-01"}

    def test_archive_digest_and_manifest_are_verified(self):
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            attestation.archive_payload(self.archive, "0" * 64, self.commit)
        with self.assertRaisesRegex(ValueError, "manifest"):
            attestation.archive_payload(self.archive, self.digest, "b" * 40)

    def test_duplicate_manifest_is_rejected(self):
        duplicate = Path(self.tmp.name) / "duplicate.tar.gz"
        with tarfile.open(self.archive, "r:gz") as source, tarfile.open(duplicate, "w:gz") as target:
            for member in source.getmembers():
                data = source.extractfile(member).read()
                target.addfile(member, io.BytesIO(data))
                if member.name.endswith("release-candidate.json"):
                    target.addfile(member, io.BytesIO(data))
        digest = hashlib.sha256(duplicate.read_bytes()).hexdigest()
        with self.assertRaisesRegex(ValueError, "duplicate candidate manifest"):
            attestation.archive_payload(duplicate, digest, self.commit)

    def test_installed_files_and_database_identity_are_bound(self):
        sums = "\n".join(f"{digest}  {path}" for path, digest in self.files.items())
        with patch.object(attestation, "docker_output", side_effect=[sums, json.dumps(self.identity)]):
            result = attestation.attest("container", self.archive, self.digest, self.commit, self.identity)
        self.assertEqual(result["payload_file_count"], 3)
        with patch.object(attestation, "docker_output", return_value=sums.replace(next(iter(self.files.values())), "0" * 64)):
            with self.assertRaisesRegex(ValueError, "payload differs"):
                attestation.attest("container", self.archive, self.digest, self.commit, self.identity)
        with patch.object(attestation, "docker_output", side_effect=[sums, json.dumps({**self.identity, "system_identifier": "other"})]):
            with self.assertRaisesRegex(ValueError, "different PostgreSQL instances"):
                attestation.attest("container", self.archive, self.digest, self.commit, self.identity)


if __name__ == "__main__":
    unittest.main()
