import unittest

from scripts.run_release_criterion import BASELINE_TAG, BASELINE_VERSION


class ReleaseCriterionTests(unittest.TestCase):
    def test_baseline_tag_and_evidence_version_are_distinct(self) -> None:
        self.assertEqual(BASELINE_TAG, "v0.105.3")
        self.assertEqual(BASELINE_VERSION, "0.105.3")


if __name__ == "__main__":
    unittest.main()
