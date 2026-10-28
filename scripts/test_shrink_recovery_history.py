"""Tests for recovery history cycle-prefix minimization."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import shrink_recovery_history as shrink


class RecoveryHistoryShrinkTests(unittest.TestCase):
    def test_only_tagged_nonzero_assertions_are_failures(self):
        output = "thread panicked: PGT_LIFECYCLE_FAILURE:exact_rows; fast/compact; history: history.json"
        self.assertEqual(shrink.failure_signature(output, 101), "exact_rows; fast/compact")
        self.assertEqual(
            shrink.failure_signature(
                "PGT_LIFECYCLE_FAILURE:source_cut; cycle=4; history: history.json", 101
            ),
            "source_cut",
        )
        self.assertIsNone(shrink.failure_signature(output, 0))
        self.assertIsNone(shrink.failure_signature("Docker daemon unavailable", 101))

    def test_finds_first_cycle_prefix_with_the_same_invariant(self):
        observed = []

        def replay(cycles):
            observed.append(cycles)
            return "exact_rows; fast/compact" if cycles >= 3 else None

        cycles, attempts = shrink.minimum_cycle_prefix(
            5, "exact_rows; fast/compact", replay
        )
        self.assertEqual(cycles, 3)
        self.assertEqual(observed, [1, 2, 3])
        self.assertEqual([attempt["cycles"] for attempt in attempts], observed)

    def test_unrelated_failure_does_not_preserve_the_target_invariant(self):
        cycles, attempts = shrink.minimum_cycle_prefix(
            4, "exact_rows; fast/compact", lambda count: "restore_pair" if count == 2 else None
        )
        self.assertEqual(cycles, 4)
        self.assertEqual(len(attempts), 3)

    def test_loads_seed_and_cycle_count_from_first_record(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            path.write_text(
                json.dumps([{"seed": 123, "cycles": 4}, {"step": "snapshot"}]),
                encoding="utf-8",
            )
            events, seed, cycles = shrink.read_history(path)
        self.assertEqual((seed, cycles), (123, 4))
        self.assertEqual(events[1]["step"], "snapshot")

    def test_shrink_keeps_original_and_reports_shortest_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            path.write_text(
                json.dumps([{"seed": 123, "cycles": 4}, {"step": "original"}]),
                encoding="utf-8",
            )

            def replay(seed, cycles, artifact_dir):
                artifact_dir.mkdir(parents=True, exist_ok=False)
                history = artifact_dir / f"history-{seed:016x}.json"
                history.write_text(
                    json.dumps([{"seed": seed, "cycles": cycles}, {"step": cycles}]),
                    encoding="utf-8",
                )
                signature = "exact_rows" if cycles >= 2 else None
                return {
                    "returncode": 101 if signature else 0,
                    "failure_signature": signature,
                    "history": str(history),
                }

            with patch.object(shrink, "run_candidate", side_effect=replay):
                original, minimized, report = shrink.shrink(path)

            self.assertEqual(json.loads(original.read_text(encoding="utf-8"))[0]["cycles"], 4)
            minimized_events = json.loads(minimized.read_text(encoding="utf-8"))
            self.assertEqual(minimized_events[0]["cycles"], 2)
            self.assertEqual(minimized_events[0]["minimization"]["failure_signature"], "exact_rows")
            report_data = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(report_data["minimized_cycles"], 2)
            self.assertEqual([attempt["cycles"] for attempt in report_data["attempts"]], [4, 1, 2])


if __name__ == "__main__":
    unittest.main()
