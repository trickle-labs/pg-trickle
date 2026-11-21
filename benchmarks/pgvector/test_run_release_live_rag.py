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
                return {"rows": rows, "dimensions": dimensions}

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
            self.assertEqual(len(report["raw_evidence_files"]), 12)


if __name__ == "__main__":
    unittest.main()
