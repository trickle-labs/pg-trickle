#!/usr/bin/env python3
"""Check that the overhead gate cannot pass with too few paired samples."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


BENCH = Path(__file__).resolve().parent


class CompareTests(unittest.TestCase):
    def compare(
        self,
        repetitions,
        minimum=3,
        installed_tps=100,
        active_p99=1,
        allow_ci_noise=True,
        throughput_noise_min=None,
    ):
        record = {
            "tps": 100,
            "transaction_latency_ms": {"p50": 1, "p95": 1, "p99": 1},
            "latency_sample_count": 1,
            "correct": True,
            "cpu": {"supported": True, "worker_share": 0},
            "postgres_version": "18",
            "postgres_settings": "same",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw.jsonl"
            rows = []
            for repetition in range(repetitions):
                for config in ("absent", "installed", "active"):
                    row = dict(record, repetition=repetition, config=config)
                    if config == "installed":
                        row["tps"] = installed_tps
                    if config == "active":
                        row["transaction_latency_ms"] = {
                            "p50": 1,
                            "p95": 1,
                            "p99": active_p99,
                        }
                    rows.append(row)
            raw.write_text("".join(json.dumps(row) + "\n" for row in rows))
            budgets = json.loads((BENCH / "budgets.json").read_text())
            budgets["ci_noise_tolerance"]["paired_repetitions_min"] = minimum
            if throughput_noise_min is not None:
                budgets["ci_noise_tolerance"]["installed_no_st_tps_ratio_min"] = throughput_noise_min
            budget_file = root / "budgets.json"
            budget_file.write_text(json.dumps(budgets))
            result_path = root / "result.json"
            command = [
                sys.executable,
                str(BENCH / "compare.py"),
                "--raw",
                str(raw),
                "--budgets",
                str(budget_file),
                "--output",
                str(result_path),
                "--expected-repetitions",
                str(repetitions),
            ]
            if allow_ci_noise:
                command.append("--allow-ci-noise")
            result = subprocess.run(
                command,
                capture_output=True, text=True, check=False,
            )
            report = json.loads(result_path.read_text()) if result_path.exists() else {}
            return result, report

    def test_requires_configured_minimum(self):
        result, _ = self.compare(1)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("at least 3 paired repetitions", result.stdout)
        self.assertEqual(self.compare(3)[0].returncode, 0)

    def test_rejects_invalid_minimum(self):
        for value in (0, True, "3"):
            with self.subTest(value=value):
                result, _ = self.compare(3, value)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must be a positive integer", result.stdout)

    def test_ci_noise_tolerates_throughput_and_latency_without_changing_product_budgets(self):
        result, report = self.compare(3, installed_tps=97.8, active_p99=2.1)
        self.assertEqual(result.returncode, 0)
        for key in ("installed_no_st_tps_ratio", "active_p99_latency_ratio"):
            with self.subTest(metric=key):
                self.assertFalse(report["decisions"][key]["product_budget_passed"])
                self.assertTrue(report["decisions"][key]["ci_noise_tolerated"])

        strict, _ = self.compare(
            3, installed_tps=97.8, active_p99=2.1, allow_ci_noise=False
        )
        self.assertNotEqual(strict.returncode, 0)
        below_noise_floor, _ = self.compare(3, installed_tps=94)
        self.assertNotEqual(below_noise_floor.returncode, 0)

    def test_rejects_ci_throughput_floor_above_product_floor(self):
        result, _ = self.compare(3, throughput_noise_min=0.995)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CI noise throughput floor", result.stdout)


if __name__ == "__main__":
    unittest.main()
