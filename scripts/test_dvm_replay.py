"""Negative-control success requires the declared detector and replay boundary."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import dvm_replay
import dvm_shrink


CONTROL = Path(__file__).resolve().parent.parent / "tests/corpus/dvm_negative_controls/neg_multiset_mismatch.json"


class NegativeControlTests(unittest.TestCase):
    def test_numeric_scale_is_compared_when_selected_by_the_query(self):
        scenario = copy.deepcopy(dvm_replay.load(CONTROL))
        scenario["query"]["stream_table"] = "output"
        scenario["query"]["defining_query"] = "SELECT id, v, scale(v) AS observed_scale FROM input"
        scenario["query"]["columns"] = ["id", "v", "scale(v) AS observed_scale"]
        with patch.object(dvm_replay, "psql", return_value="ok") as query:
            dvm_replay.exact_check(scenario, "unused")
            sql = query.call_args.args[1]
        self.assertEqual(sql.count("scale(v)"), 6)

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
            return "INSERT 0 2" if "VALUES (3, 1, 25), (4, 2, 10)" in sql else "UPDATE 1"

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

    def test_transaction_script_preserves_boundaries_and_counts_dml(self):
        script = "BEGIN; UPDATE t SET v=1; SAVEPOINT s; UPDATE t SET v=2; ROLLBACK TO s; COMMIT"
        with patch.object(dvm_replay, "psql", return_value="BEGIN\nUPDATE 1\nSAVEPOINT\nUPDATE 1\nROLLBACK\nCOMMIT") as run:
            self.assertEqual(dvm_replay.execute_mutation("db", script), 2)
            run.assert_called_once_with("db", script)

    def test_fallback_or_missing_history_cannot_qualify_semantic_floor(self):
        scenario = dvm_replay.load(CONTROL)
        with patch.object(dvm_replay, "psql", return_value="f"):
            with self.assertRaisesRegex(dvm_replay.ReplayFailure, "fresh completed differential"):
                dvm_replay.changed_route_check(scenario, "db", 12)

    def test_shrinking_preserves_typed_transaction_sql(self):
        scenario = copy.deepcopy(dvm_replay.load(CONTROL))
        scenario["preserve_sql_boundaries"] = True
        transaction = "BEGIN; INSERT INTO t VALUES (1,1.00); SAVEPOINT s; UPDATE t SET v='Infinity'; ROLLBACK TO s; COMMIT"
        scenario["cycles"] = [{"name": "boundary", "mutations": [{"sql": transaction, "expected_affected_rows": 2}]}]
        before_schema = copy.deepcopy(scenario["schema"])
        reduced = dvm_shrink.shrink_scenario(scenario, lambda _: True)
        self.assertEqual(reduced["schema"], before_schema)
        self.assertEqual(reduced["query"], scenario["query"])
        self.assertEqual(reduced["cycles"][0]["mutations"][0]["sql"], transaction)

    def test_capture_and_consumed_cut_are_required_without_semantic_retries(self):
        scenario = dvm_replay.load(CONTROL)
        with patch.object(dvm_replay, "psql", side_effect=['[[42,"pgtrickle_changes.changes_x"]]', '[[42,"0/ABCD"]]']):
            cuts = dvm_replay.captured_source_cuts(scenario, "db")
        self.assertEqual(cuts, [(42, "0/ABCD")])
        with patch.object(dvm_replay, "psql", return_value="f") as run:
            with self.assertRaisesRegex(dvm_replay.ReplayFailure, "CoverageNotReached.*earlier cut"):
                dvm_replay.source_cut_check(scenario, "db", cuts)
            self.assertEqual(run.call_count, 1)
        with patch.object(dvm_replay, "psql", return_value="[]"):
            with self.assertRaisesRegex(dvm_replay.ReplayFailure, "no captured sources"):
                dvm_replay.captured_source_cuts(scenario, "db")


if __name__ == "__main__":
    unittest.main()
