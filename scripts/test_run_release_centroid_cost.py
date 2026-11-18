import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_release_centroid_cost as runner


class CentroidCostMeasurementTests(unittest.TestCase):
    def measurement(self, dimensions=384, changed_rows=1, observations=None, cache_mode="warm", concurrent_writer=False):
        report = {
            "dimensions": dimensions,
            "rows": runner.ROWS,
            "centroid_repeated": {
                "measured_rounds": runner.SAMPLES,
                "warmup_rounds": runner.WARMUP,
                "changed_rows_per_round": changed_rows,
                "cache_mode": cache_mode,
                "concurrent_writer": concurrent_writer,
                "lock_wait_ms": {"p50": 12.0} if concurrent_writer else None,
                "cache_reset_ms": {"p50": 1.0} if cache_mode == "cold" else None,
                "observations": (
                    [
                        {
                            "cache_mode": cache_mode,
                            "concurrent_writer": concurrent_writer,
                            "writer_lock_wait_ms": 0.0 if concurrent_writer else None,
                            "writer_transaction_ms": 12.0 if concurrent_writer else None,
                            "cache_reset_ms": 1.0 if cache_mode == "cold" else None,
                        }
                    ]
                    * (4 * 3 * runner.SAMPLES)
                    if observations is None
                    else observations
                ),
            },
        }
        return runner.MARKER + json.dumps(report)

    def test_accepts_complete_release_matrix_case(self):
        report = runner.parse_measurement(self.measurement(), 384)
        self.assertEqual(report["centroid_repeated"]["measured_rounds"], runner.SAMPLES)
        concurrent = self.measurement(
            cache_mode="warm",
            concurrent_writer=True,
            observations=[
                {
                    "cache_mode": "warm",
                    "concurrent_writer": True,
                    "writer_lock_wait_ms": 0.0,
                    "writer_transaction_ms": 12.0,
                }
            ]
            * (4 * 3 * runner.SAMPLES),
        )
        runner.parse_measurement(concurrent, 384, "warm", True)

    def test_release_matrix_covers_cache_and_writer_modes(self):
        self.assertEqual(
            runner.MODES,
            (("warm", False), ("cold", False), ("warm", True)),
        )
        contract = json.loads(
            (runner.ROOT / "tests/release/v0.109.0-qualification.json").read_text(encoding="utf-8")
        )
        suite = next(item for item in contract["required_suites"] if item["id"] == "centroid-cost-matrix")
        self.assertEqual(
            suite["required_modes"],
            [{"cache_mode": mode, "concurrent_writer": writer} for mode, writer in runner.MODES],
        )

    def test_rejects_missing_cache_or_lock_wait_evidence(self):
        with self.assertRaises(ValueError):
            runner.parse_measurement(self.measurement(cache_mode="warm"), 384, "cold", False)
        with self.assertRaises(ValueError):
            runner.parse_measurement(
                self.measurement(
                    cache_mode="warm",
                    concurrent_writer=True,
                    observations=[{"writer_lock_wait_ms": 12.0}] * (4 * 3 * runner.SAMPLES),
                ),
                384,
                "warm",
                True,
            )

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
        def fail_case(dimensions, changed_rows, cache_mode, concurrent_writer, measurement_path):
            writer_mode = "concurrent" if concurrent_writer else "serial"
            stem = f"centroid-cost-{cache_mode}-{writer_mode}-{dimensions}-batch-{changed_rows}"
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
                    measurement.with_name("centroid-cost-warm-serial-384-batch-1.log"),
                    measurement.with_name("centroid-cost-warm-serial-384-batch-1-docker-stats.jsonl"),
                )
            )
            self.assertEqual(report["raw_evidence_files"], expected)


if __name__ == "__main__":
    unittest.main()
