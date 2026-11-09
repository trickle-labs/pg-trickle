import io
import tarfile
import tempfile
import unittest
import warnings
import zipfile
from pathlib import Path

import check_ann_release_payload as check
import run_release_suite


class AnnReleasePayloadTests(unittest.TestCase):
    def test_release_extraction_retains_operator_scripts(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            package = directory / "package"
            for name in ("lib", "extension", "scripts"):
                (package / name).mkdir(parents=True, exist_ok=True)
            (package / "lib/pg_trickle.so").write_bytes(b"library")
            (package / "extension/pg_trickle.control").write_text("control", encoding="utf-8")
            (package / "scripts/ann_maintenance_worker.sql").write_bytes(b"worker")
            archive = directory / "candidate.tar.gz"
            with tarfile.open(archive, "w:gz") as payload:
                payload.add(package, arcname="package")
            destination = directory / "result"
            destination.mkdir()
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning, module="run_release_suite")
                extracted = run_release_suite.package_directory(archive, destination)
            self.assertEqual((extracted / "scripts/ann_maintenance_worker.sql").read_bytes(), b"worker")

    def test_both_archive_formats_reject_missing_changed_and_duplicate_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for kind in ("tar.gz", "zip"):
                for mutation in (None, "missing", "changed", "duplicate"):
                    with self.subTest(kind=kind, mutation=mutation):
                        archive = directory / f"candidate.{kind}"
                        paths = list(check.FILES)
                        if mutation == "missing":
                            paths.pop()
                        if mutation == "duplicate":
                            paths.append(paths[0])
                        if kind == "tar.gz":
                            with tarfile.open(archive, "w:gz") as payload:
                                for index, path in enumerate(paths):
                                    data = (check.ROOT / path).read_bytes()
                                    if mutation == "changed" and index == 0:
                                        data += b"modified"
                                    member = tarfile.TarInfo(f"candidate/{path}")
                                    member.size = len(data)
                                    payload.addfile(member, io.BytesIO(data))
                        else:
                            with warnings.catch_warnings(), zipfile.ZipFile(archive, "w") as payload:
                                warnings.simplefilter("ignore", UserWarning)
                                for index, path in enumerate(paths):
                                    data = (check.ROOT / path).read_bytes()
                                    if mutation == "changed" and index == 0:
                                        data += b"modified"
                                    payload.writestr(f"candidate/{path}", data)
                        if mutation is None:
                            check.check_archive(archive)
                        else:
                            with self.assertRaises(ValueError):
                                check.check_archive(archive)


if __name__ == "__main__":
    unittest.main()
