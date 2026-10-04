import json
import unittest

from pathlib import Path

from scripts.run_release_criterion import criterion_baseline


class ReleaseCriterionTests(unittest.TestCase):
    def test_baseline_uses_explicit_previous_release(self) -> None:
        contract = json.loads(
            (Path(__file__).resolve().parents[1] / "tests/release/v0.108.2-qualification.json")
            .read_text(encoding="utf-8")
        )
        self.assertEqual(criterion_baseline(contract), "0.108.1")

    def test_baseline_must_be_a_declared_source_version(self) -> None:
        contract = {
            "performance_budgets": [{"id": "criterion-regression", "baseline_version": "0.108.1"}],
            "source_versions": ["0.106.1"],
        }
        with self.assertRaisesRegex(ValueError, "declared source version"):
            criterion_baseline(contract)


if __name__ == "__main__":
    unittest.main()
