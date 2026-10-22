"""Negative-control success requires the declared detector and replay boundary."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import dvm_replay


CONTROL = Path(__file__).resolve().parent.parent / "tests/corpus/dvm_negative_controls/neg_multiset_mismatch.json"


class NegativeControlTests(unittest.TestCase):
    def test_every_failure_coordinate_must_match(self):
        scenario = dvm_replay.load(CONTROL)
        failure = {
            key.removeprefix("expected_"): value
            for key, value in scenario["negative_control"].items()
            if key.startswith("expected_")
        }
        self.assertTrue(dvm_replay.matches_negative_control(scenario, failure))
        for key in failure:
            changed = {**failure, key: "wrong"}
            self.assertFalse(dvm_replay.matches_negative_control(scenario, changed), key)

    def test_failure_phase_is_required(self):
        scenario = copy.deepcopy(dvm_replay.load(CONTROL))
        del scenario["negative_control"]["expected_phase"]
        with self.assertRaisesRegex(ValueError, "expected_phase is required"):
            dvm_replay.validate(scenario)

    def test_setup_failure_is_not_semantic_detection(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ", {"DVM_ARTIFACT_DIR": directory}
        ), patch.object(
            dvm_replay, "psql", side_effect=RuntimeError("MultisetMismatch: setup failed")
        ):
            with self.assertRaisesRegex(RuntimeError, "setup failed"):
                dvm_replay.run(CONTROL, "unused", expect_negative_control=True)
            failure = json.loads(next(Path(directory).glob("*/failure.json")).read_text())
            self.assertEqual(failure["failure_class"], "ReplayError")
            self.assertEqual(failure["phase"], "schema setup")

    def test_expected_semantic_failure_and_clean_result_are_distinguished(self):
        def psql(_database, sql, tuples=False):
            return "2" if "VALUES (3, 1, 25), (4, 2, 10)" in sql else "1"

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ", {"DVM_ARTIFACT_DIR": directory}
        ), patch.object(dvm_replay, "psql", side_effect=psql), patch.object(
            dvm_replay, "mode_check"
        ), patch.object(dvm_replay, "exact_check") as exact:
            exact.side_effect = [
                None, None, dvm_replay.ReplayFailure("MultisetMismatch", "corruption")
            ]
            dvm_replay.run(CONTROL, "unused", expect_negative_control=True)
            exact.side_effect = None
            # Correct output passes ordinary replay but cannot count as killing a mutant.
            dvm_replay.run(CONTROL, "unused")
            with self.assertRaisesRegex(RuntimeError, "passed without its declared failure"):
                dvm_replay.run(CONTROL, "unused", expect_negative_control=True)


if __name__ == "__main__":
    unittest.main()
