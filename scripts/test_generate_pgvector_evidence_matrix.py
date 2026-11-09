import tempfile
import unittest
from pathlib import Path

import generate_pgvector_evidence_matrix as matrix


class PgvectorEvidenceMatrixTests(unittest.TestCase):
    def test_requires_every_named_test_and_successful_suite_on_both_versions(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for suite in {case[2] for case in matrix.CASES}:
                tests = {
                    test
                    for _, _, name, names in matrix.CASES
                    if name == suite
                    for test in names
                }
                for version in matrix.VERSIONS:
                    content = "".join(f"test {name} ... ok\n" for name in sorted(tests))
                    (directory / f"{suite}-{version}.log").write_text(
                        content
                        + f"test result: ok. {len(tests)} passed; 0 failed; 0 ignored;\n"
                    )
            self.assertIn("| Algebraic vector AVG |", matrix.render(directory))
            path = directory / "conformance-0.8.1.log"
            content = path.read_text()
            path.write_text(
                content.replace(
                    "test_centroid_opt_in_applies_and_falls_back_after_corruption ... ok",
                    "test_centroid_opt_in_applies_and_falls_back_after_corruption ... FAILED",
                )
            )
            with self.assertRaisesRegex(ValueError, "missing successful"):
                matrix.render(directory)
            path.write_text(content.replace("0 failed;", "1 failed;"))
            with self.assertRaisesRegex(ValueError, "no successful suite result"):
                matrix.render(directory)
            path.write_text(
                content + "test result: FAILED. 0 passed; 1 failed; 0 ignored;\n"
            )
            with self.assertRaisesRegex(ValueError, "no successful suite result"):
                matrix.render(directory)


if __name__ == "__main__":
    unittest.main()
