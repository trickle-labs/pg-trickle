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

    def test_reader_query_uses_requested_vector_dimensions(self):
        sql = benchmark.reader_sql(768)
        self.assertIn("vector(768)", sql)
        self.assertNotIn("vector(3)", sql)
        self.assertIn("server_started", sql)

    def test_reader_overlap_and_timeout_windows_are_reported_separately(self):
        reads = [
            {"server_started": 1, "server_finished": 2, "client_seconds": 0.2},
            {"server_started": 3, "server_finished": 5, "client_seconds": 0.4},
            {"timed_out": True, "client_started": 10, "client_finished": 40},
            {"timed_out": True, "client_started": 50, "client_finished": 80},
        ]
        report = benchmark.reader_summary(reads, (20, 30), 2, 4, 1)
        self.assertEqual(report["queries"], 2)
        self.assertEqual(report["overlapping_queries"], 1)
        self.assertEqual(report["timed_out_queries"], 2)
        self.assertEqual(report["timeouts_overlapping_maintenance_client_window"], 1)
        self.assertEqual(report["overlapping_client_latency_seconds"]["p50"], 0.4)

    def test_mixed_workload_rejects_reused_delete_ids_before_connecting(self):
        result = subprocess.run(
            [sys.executable, benchmark.__file__, "--rows", "4", "--dimensions", "384",
             "--batch", "4", "--cycles", "2", "--workload", "mixed",
             "--candidate-commit", "0" * 40, "--package-sha256", "0" * 64],
            capture_output=True, text=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("deleted IDs are not reused", result.stderr)

    def test_attestation_requires_both_archive_and_container(self):
        result = subprocess.run(
            [sys.executable, benchmark.__file__, "--rows", "4", "--dimensions", "384",
             "--candidate-commit", "0" * 40, "--package-sha256", "0" * 64,
             "--candidate-archive", "candidate.tar.gz"],
            capture_output=True, text=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be supplied together", result.stderr)


if __name__ == "__main__":
    unittest.main()
