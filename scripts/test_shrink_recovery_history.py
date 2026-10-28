"""Tests for recovery history minimization."""

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

    def test_reads_structured_and_legacy_mutation_records(self):
        events = [
            {
                "step": "committed source+journal mutation",
                "cycle": 1,
                "mutation": {"kind": "update", "id": 0, "value": -9},
            },
            {
                "step": "committed source+journal mutation",
                "cycle": 2,
                "sql": "BEGIN; INSERT INTO history_source VALUES (2,'7'); INSERT INTO history_journal VALUES (2,'7'); COMMIT",
            },
            {
                "step": "committed source+journal mutation",
                "cycle": 3,
                "sql": "BEGIN; DELETE FROM history_source WHERE id=2; DELETE FROM history_journal WHERE id=2; COMMIT",
            },
        ]
        self.assertEqual(
            shrink.mutation_plan(events),
            [
                {"kind": "update", "id": 0, "value": -9, "source_cycle": 1},
                {"kind": "insert", "id": 2, "value": 7, "source_cycle": 2},
                {"kind": "delete", "id": 2, "value": 0, "source_cycle": 3},
            ],
        )

    def test_shrink_removes_arbitrary_cycles_and_zeroes_operands(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            original_mutations = [
                {"kind": "insert", "id": 1, "value": 11, "source_cycle": 1},
                {"kind": "update", "id": 1, "value": 12, "source_cycle": 2},
                {"kind": "delete", "id": 1, "value": 13, "source_cycle": 3},
                {"kind": "insert", "id": 4, "value": 14, "source_cycle": 4},
            ]
            def replay(seed, mutations, artifact_dir):
                artifact_dir.mkdir(parents=True, exist_ok=False)
                (artifact_dir / "mutation-plan.json").write_text(
                    json.dumps(mutations), encoding="utf-8"
                )
                history = artifact_dir / f"history-{seed:016x}.json"
                events = [{"seed": seed, "cycles": len(mutations)}]
                events.extend(
                    {
                        "step": "committed source+journal mutation",
                        "cycle": operation["source_cycle"],
                        "mutation": operation,
                    }
                    for operation in mutations
                )
                history.write_text(
                    json.dumps(events),
                    encoding="utf-8",
                )
                signature = "exact_rows" if any(op["kind"] == "delete" for op in mutations) else None
                return {
                    "returncode": 101 if signature else 0,
                    "failure_signature": signature,
                    "history": str(history),
                    "replay_plan": str(artifact_dir / "mutation-plan.json"),
                }

            path.write_text(
                json.dumps(
                    [
                        {"seed": 123, "cycles": 4},
                        *[
                            {
                                "step": "committed source+journal mutation",
                                "cycle": operation["source_cycle"],
                                "mutation": operation,
                            }
                            for operation in original_mutations
                        ],
                    ]
                ),
                encoding="utf-8",
            )
            with patch.object(shrink, "run_candidate", side_effect=replay):
                original, minimized, report = shrink.shrink(path)

            self.assertEqual(json.loads(original.read_text(encoding="utf-8"))[0]["cycles"], 4)
            minimized_events = json.loads(minimized.read_text(encoding="utf-8"))
            self.assertEqual(minimized_events[0]["cycles"], 1)
            self.assertEqual(
                minimized_events[0]["minimization"]["failure_signature"], "exact_rows"
            )
            self.assertEqual(
                minimized_events[0]["minimization"]["minimized_mutations"],
                [{"kind": "delete", "id": 0, "value": 13, "source_cycle": 3}],
            )
            report_data = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(report_data["minimized_cycles"], 1)
            self.assertEqual(report_data["minimized_mutations"][0]["source_cycle"], 3)
            self.assertTrue(
                any(attempt["source_cycles"] == [1, 3] for attempt in report_data["attempts"])
            )


if __name__ == "__main__":
    unittest.main()
