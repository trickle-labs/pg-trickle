import json
import unittest

from scripts import run_release_criterion as criterion


class ReleaseCriterionTests(unittest.TestCase):
    def test_baseline_matches_previous_release_in_current_contract(self) -> None:
        criterion.configure_release("0.109.0")
        self.assertEqual(criterion.BASELINE_TAG, "v0.108.3")
        self.assertEqual(criterion.BASELINE_VERSION, "0.108.3")
        self.assertEqual(criterion.QUALIFICATION.name, "v0.109.0-qualification.json")
        contract = json.loads(criterion.QUALIFICATION.read_text(encoding="utf-8"))
        suite = next(s for s in contract["required_suites"] if s["id"] == "criterion-regression")
        self.assertIn(f"v{criterion.BASELINE_VERSION}", suite["workload"])

    def test_missing_contract_does_not_fall_back_to_an_older_release(self) -> None:
        with self.assertRaises(FileNotFoundError):
            criterion.configure_release("999.0.0")


if __name__ == "__main__":
    unittest.main()
