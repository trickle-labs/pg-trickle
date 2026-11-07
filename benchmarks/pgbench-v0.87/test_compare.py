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
    def compare(self, repetitions, minimum=3):
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
            raw.write_text("".join(
                json.dumps(dict(record, repetition=i, config=config)) + "\n"
                for i in range(repetitions)
                for config in ("absent", "installed", "active")
            ))
            budgets = json.loads((BENCH / "budgets.json").read_text())
            budgets["ci_noise_tolerance"]["paired_repetitions_min"] = minimum
            budget_file = root / "budgets.json"
            budget_file.write_text(json.dumps(budgets))
            return subprocess.run(
                [sys.executable, str(BENCH / "compare.py"), "--raw", str(raw),
                 "--budgets", str(budget_file), "--output", str(root / "result.json"),
                 "--expected-repetitions", str(repetitions), "--allow-ci-noise"],
                capture_output=True, text=True, check=False,
            )

    def test_requires_configured_minimum(self):
        result = self.compare(1)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("at least 3 paired repetitions", result.stdout)
        self.assertEqual(self.compare(3).returncode, 0)

    def test_rejects_invalid_minimum(self):
        for value in (0, True, "3"):
            with self.subTest(value=value):
                result = self.compare(3, value)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must be a positive integer", result.stdout)


if __name__ == "__main__":
    unittest.main()
