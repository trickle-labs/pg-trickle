import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_release_centroid_cost as runner


class CentroidCostMeasurementTests(unittest.TestCase):
    def measurement(self, dimensions=384, changed_rows=1, observations=None):
        report = {
            "dimensions": dimensions,
            "rows": runner.ROWS,
            "centroid_repeated": {
                "measured_rounds": runner.SAMPLES,
                "warmup_rounds": runner.WARMUP,
                "changed_rows_per_round": changed_rows,
                "observations": [{}] * (4 * 3 * runner.SAMPLES) if observations is None else observations,
            },
        }
        return runner.MARKER + json.dumps(report)

    def test_accepts_complete_release_matrix_case(self):
        report = runner.parse_measurement(self.measurement(), 384)
        self.assertEqual(report["centroid_repeated"]["measured_rounds"], runner.SAMPLES)

    def test_rejects_missing_or_duplicate_result_markers(self):
        with self.assertRaises(ValueError):
            runner.parse_measurement("", 384)
        record = self.measurement()
        with self.assertRaises(ValueError):
            runner.parse_measurement(record + "\n" + record, 384)

    def test_rejects_wrong_shape_or_incomplete_observations(self):
        with self.assertRaises(ValueError):
            runner.parse_measurement(self.measurement(dimensions=1536), 384)
        with self.assertRaises(ValueError):
            runner.parse_measurement(self.measurement(observations=[]), 384)
        with self.assertRaises(ValueError):
            runner.parse_measurement(self.measurement(changed_rows=2), 384)

    def test_failed_case_retains_partial_raw_evidence(self):
        def fail_case(dimensions, changed_rows, measurement_path):
            stem = f"centroid-cost-{dimensions}-batch-{changed_rows}"
            measurement_path.with_name(f"{stem}.log").write_text("failed run", encoding="utf-8")
            measurement_path.with_name(f"{stem}-docker-stats.jsonl").write_text("{}\n", encoding="utf-8")
            raise RuntimeError("benchmark failed")

        with tempfile.TemporaryDirectory(dir=runner.ROOT) as directory:
            measurement = Path(directory) / "centroid-cost-matrix.json"
            with patch.dict(
                os.environ, {"PGS_RELEASE_MEASUREMENT_JSON": str(measurement)}
            ), patch.object(
                runner, "DIMENSIONS", (384,)
            ), patch.object(
                runner, "REWRITES", (1,)
            ), patch.object(
                runner, "run_case", side_effect=fail_case
            ):
                with self.assertRaises(RuntimeError):
                    runner.main()

            report = json.loads(measurement.read_text(encoding="utf-8"))
            expected = sorted(
                path.relative_to(runner.ROOT).as_posix()
                for path in (
                    measurement.with_name("centroid-cost-384-batch-1.log"),
                    measurement.with_name("centroid-cost-384-batch-1-docker-stats.jsonl"),
                )
            )
            self.assertEqual(report["raw_evidence_files"], expected)


if __name__ == "__main__":
    unittest.main()
