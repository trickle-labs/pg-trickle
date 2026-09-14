import json
import tempfile
import unittest
from pathlib import Path

from scripts.criterion_regression_check import find_bench_pairs


class CriterionRegressionCheckTests(unittest.TestCase):
    def test_named_baseline_pairs_criterion_point_estimates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            criterion_dir = Path(tmp) / "criterion"
            benchmark_dir = criterion_dir / "group" / "benchmark"
            baseline = benchmark_dir / "v0.105.3" / "estimates.json"
            candidate = benchmark_dir / "new" / "estimates.json"
            baseline.parent.mkdir(parents=True)
            candidate.parent.mkdir(parents=True)
            baseline.write_text(json.dumps({"mean": {"point_estimate": 100.0}}))
            candidate.write_text(json.dumps({"mean": {"point_estimate": 109.0}}))

            self.assertEqual(
                find_bench_pairs(criterion_dir, "v0.105.3"),
                [("group/benchmark", 100.0, 109.0)],
            )


if __name__ == "__main__":
    unittest.main()
