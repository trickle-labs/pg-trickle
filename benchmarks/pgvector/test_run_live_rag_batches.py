import unittest
import subprocess
import sys

import run_live_rag_batches as benchmark


class BenchmarkTests(unittest.TestCase):
    def test_nearest_rank_summary(self):
        self.assertEqual(benchmark.summary([3.0, 1.0, 2.0]),
                         {"samples": 3, "p50": 2.0, "p95": 3.0, "p99": 3.0})

    def test_json_result_ignores_prior_psql_output(self):
        self.assertEqual(benchmark.json_result('\n{"ok":true}\n'), {"ok": True})

    def test_mixed_workload_rejects_reused_delete_ids_before_connecting(self):
        result = subprocess.run(
            [sys.executable, benchmark.__file__, "--rows", "4", "--dimensions", "384",
             "--batch", "4", "--cycles", "2", "--workload", "mixed",
             "--candidate-commit", "0" * 40, "--package-sha256", "0" * 64],
            capture_output=True, text=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("deleted IDs are not reused", result.stderr)


if __name__ == "__main__":
    unittest.main()
