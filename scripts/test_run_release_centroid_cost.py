import json
import unittest

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


if __name__ == "__main__":
    unittest.main()
