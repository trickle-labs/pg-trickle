import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_release_live_rag


class ReleaseLiveRagTests(unittest.TestCase):
    def test_release_live_rag_supports_10k_100k_and_all_dimensions_but_not_1m(self):
        parsed = run_release_live_rag.parser().parse_args(
            ["--rows", "10000", "100000", "--dimensions", "384", "768", "1536"]
        )
        self.assertEqual(tuple(parsed.rows), (10_000, 100_000))
        self.assertEqual(tuple(parsed.dimensions), (384, 768, 1536))
        with self.assertRaises(SystemExit):
            run_release_live_rag.parser().parse_args(
                ["--rows", "1000000", "--dimensions", "384", "768", "1536"]
            )

    def test_release_live_rag_records_all_six_matrix_cases(self):
        with tempfile.TemporaryDirectory(dir=run_release_live_rag.ROOT) as directory:
            measurement = Path(directory) / "measurement.json"

            def run_case(rows, dimensions, path):
                size = f"{rows // 1000}k"
                for suffix in ("docker-stats.jsonl", "checkpoint.json"):
                    path.with_name(f"live-rag-{size}-{dimensions}-{suffix}").write_text(
                        "{}", encoding="utf-8"
                    )
                metrics = {
                    "recall_at_k": 0.9,
                    "forced_recall_at_k": 0.8,
                    "hybrid_recall_at_10": 1.0,
                    "hybrid_underfill": 0,
                }
                return {
                    "rows": rows,
                    "dimensions": dimensions,
                    "cycles": 3,
                    "candidate_commit": "a" * 40,
                    "package_sha256": "b" * 64,
                    "metadata": {"postgres": "18.6", "pgvector": "0.8.7", "pg_trickle": "0.109.0"},
                    "initial_corpora_exact": {"application": True, "periodic_full": True, "differential": True},
                    "batch_latency_seconds": {
                        "application": {"p50": 1.0},
                        "differential": {"p50": 2.0},
                        "periodic_full": {"p50": 10.0},
                    },
                    "samples": [{
                        "corpora_exact": {"application": True, "periodic_full": True, "differential": True},
                        "retrieval": {
                            name: {
                                "metrics": metrics,
                                "planner_used_ann_index": name == "differential",
                                "forced_plan_used_ann_index": name == "differential",
                            }
                            for name in ("application", "differential", "periodic_full")
                        },
                    }],
                }

            with patch.dict(
                os.environ,
                {
                    "PGS_RELEASE_MEASUREMENT_JSON": str(measurement),
                    "PGT_RELEASE_CANDIDATE_COMMIT": "a" * 40,
                    "PGT_RELEASE_ARTIFACT_SHA256": "b" * 64,
                },
            ), patch(
                "sys.argv",
                ["runner", "--rows", "10000", "100000", "--dimensions", "384", "768", "1536"],
            ), patch.object(
                run_release_live_rag, "run_case", side_effect=run_case
            ) as run:
                self.assertEqual(run_release_live_rag.main(), 0)

            expected_cases = [
                (rows, dimensions)
                for rows in (10_000, 100_000)
                for dimensions in (384, 768, 1536)
            ]
            self.assertEqual(
                [(call.args[0], call.args[1]) for call in run.call_args_list],
                expected_cases,
            )
            report = json.loads(measurement.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "complete")
            self.assertEqual(len(report["raw_evidence_files"]), 15)
            maintenance = report["decision_table"]["maintenance"]
            self.assertEqual(len(maintenance), 6)
            self.assertEqual(maintenance[0]["differential_comparison"]["application"]["outcome"], "loses")
            self.assertEqual(maintenance[0]["differential_comparison"]["periodic_full"]["outcome"], "wins")
            self.assertEqual(len(report["decision_table"]["retrieval"]), 18)
            for name in report["raw_evidence_files"]:
                self.assertTrue((run_release_live_rag.ROOT / name).is_file(), name)
            table = run_release_live_rag.ROOT / report["decision_table_file"]
            self.assertIn("Differential vs periodic full", table.read_text(encoding="utf-8"))
            self.assertEqual(len(report["plot_files"]), 2)
            case = report["cases"][0]
            case["samples"][0]["retrieval"]["differential"]["metrics"]["forced_recall_at_k"] = None
            differential = next(
                row for row in run_release_live_rag.decision_table([case])["retrieval"]
                if row["corpus"] == "differential"
            )
            self.assertEqual(differential["forced_hnsw_samples"], 1)
            self.assertIsNone(differential["forced_ann_recall_at_10_p50"])


if __name__ == "__main__":
    unittest.main()
