import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.criterion_regression_check import find_bench_pairs, is_material_regression, main


class CriterionRegressionCheckTests(unittest.TestCase):
    def test_named_baseline_pairs_criterion_point_estimates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            criterion_dir = Path(tmp) / "criterion"
            benchmark_dir = criterion_dir / "group" / "benchmark"
            baseline = benchmark_dir / "0.105.3" / "estimates.json"
            candidate = benchmark_dir / "new" / "estimates.json"
            baseline.parent.mkdir(parents=True)
            candidate.parent.mkdir(parents=True)
            baseline.write_text(json.dumps({"mean": {"point_estimate": 100.0}}))
            candidate.write_text(json.dumps({"mean": {"point_estimate": 109.0}}))

            self.assertEqual(
                find_bench_pairs(criterion_dir, "0.105.3"),
                [("group/benchmark", 100.0, 109.0)],
            )

    def test_materiality_floor_keeps_small_percentage_changes_visible_but_ungated(self) -> None:
        self.assertFalse(is_material_regression(12.125, 14.929, 0.10, 50.0))
        self.assertFalse(is_material_regression(324.868, 366.041, 0.10, 50.0))
        self.assertFalse(is_material_regression(121.873, 138.091, 0.10, 50.0))
        self.assertTrue(is_material_regression(100.0, 151.0, 0.10, 50.0))

    def test_json_reports_subfloor_regression_and_raw_maximum(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            criterion_dir = Path(tmp) / "criterion"
            benchmark_dir = criterion_dir / "frontier_json" / "serialize_1"
            baseline = benchmark_dir / "0.105.3" / "estimates.json"
            candidate = benchmark_dir / "new" / "estimates.json"
            baseline.parent.mkdir(parents=True)
            candidate.parent.mkdir(parents=True)
            baseline.write_text(json.dumps({"mean": {"point_estimate": 121.873}}))
            candidate.write_text(json.dumps({"mean": {"point_estimate": 138.091}}))
            output = Path(tmp) / "comparison.json"

            with patch(
                "sys.argv",
                [
                    "criterion_regression_check.py",
                    "--threshold", "0.10",
                    "--minimum-delta-ns", "50",
                    "--baseline", "0.105.3",
                    "--require-pairs",
                    "--dir", str(criterion_dir),
                    "--json-output", str(output),
                ],
            ):
                self.assertEqual(main(), 0)

            result = json.loads(output.read_text())
            self.assertEqual(result["maximum_mean_regression_pct"], 0.0)
            self.assertEqual(result["minimum_absolute_delta_ns"], 50.0)
            self.assertAlmostEqual(result["maximum_raw_mean_regression_pct"], 13.307, places=2)
            self.assertEqual(len(result["subfloor_regressions"]), 1)


if __name__ == "__main__":
    unittest.main()
