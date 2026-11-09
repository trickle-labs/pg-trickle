import unittest

import run_live_rag_batches as benchmark


class BenchmarkTests(unittest.TestCase):
    def test_nearest_rank_summary(self):
        self.assertEqual(benchmark.summary([3.0, 1.0, 2.0]),
                         {"samples": 3, "p50": 2.0, "p95": 3.0, "p99": 3.0})

    def test_json_result_ignores_prior_psql_output(self):
        self.assertEqual(benchmark.json_result('\n{"ok":true}\n'), {"ok": True})


if __name__ == "__main__":
    unittest.main()
