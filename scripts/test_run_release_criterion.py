import unittest

from scripts.run_release_criterion import BASELINE_TAG, BASELINE_VERSION, QUALIFICATION


class ReleaseCriterionTests(unittest.TestCase):
    def test_baseline_matches_previous_release_in_current_contract(self) -> None:
        self.assertEqual(BASELINE_TAG, "v0.108.2")
        self.assertEqual(BASELINE_VERSION, "0.108.2")
        self.assertEqual(QUALIFICATION.name, "v0.108.3-qualification.json")


if __name__ == "__main__":
    unittest.main()
