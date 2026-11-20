import unittest
import subprocess
import sys
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import run_live_rag_batches as benchmark


class BenchmarkTests(unittest.TestCase):
    def test_checkpoint_retains_completed_measurements_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "progress.json"
            checkpoint = benchmark.CheckpointWriter(path)
            checkpoint.update(
                {"phase": "maintenance", "approach": "application", "state": "complete"},
                samples=[{"cycle": 0, "maintenance": {"application": {"duration_seconds": 1.0}}}],
            )
            checkpoint.fail(ValueError("periodic full stopped"))

            report = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["error"], "ValueError: periodic full stopped")
            self.assertEqual(report["progress"]["approach"], "application")
            self.assertEqual(report["samples"][0]["maintenance"]["application"]["duration_seconds"], 1.0)
            self.assertEqual(list(path.parent.glob(".*.tmp")), [])

    def test_checkpoint_marks_a_complete_report(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.json"
            benchmark.CheckpointWriter(path).complete({"rows": 100, "samples": []})

            report = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["progress"], {"phase": "complete"})

    def test_nearest_rank_summary(self):
        self.assertEqual(benchmark.summary([3.0, 1.0, 2.0]),
                         {"samples": 3, "p50": 2.0, "p95": 3.0, "p99": 3.0})

    def test_json_result_ignores_prior_psql_output(self):
        self.assertEqual(benchmark.json_result('\n{"ok":true}\n'), {"ok": True})

    def test_database_metadata_installs_vector_before_reading_versions(self):
        versions = {
            "postgres": "18.6",
            "pgvector": "0.8.7",
            "pg_trickle": "0.109.0",
            "occupied": False,
        }
        with patch.object(
            benchmark.worker,
            "psql",
            side_effect=["CREATE EXTENSION", json.dumps(versions)],
        ) as psql:
            self.assertEqual(benchmark.database_metadata(), versions)

        self.assertEqual(psql.call_args_list[0].args[0], "CREATE EXTENSION IF NOT EXISTS vector")
        self.assertIn("FROM pg_extension WHERE extname='vector'", psql.call_args_list[1].args[0])

    def test_plan_execution_time_is_extracted_from_explain(self):
        self.assertEqual(benchmark.plan_execution_ms({"plans": {"planner": [{"Execution Time": 1.25}]}}, "planner"), 1.25)

    def test_reader_query_uses_requested_vector_dimensions(self):
        sql = benchmark.reader_sql(768)
        self.assertIn("vector(768)", sql)
        self.assertNotIn("vector(3)", sql)
        self.assertIn("server_started", sql)

    def test_exact_comparison_disables_parallel_workers_and_pauses_all_fixture_streams(self):
        self.assertTrue(benchmark.EXACT.lstrip().startswith("BEGIN;"))
        self.assertIn("SET LOCAL max_parallel_workers_per_gather = 0;", benchmark.EXACT)
        self.assertIn("SET LOCAL work_mem = '16MB';", benchmark.EXACT)
        self.assertIn("WITH truth AS MATERIALIZED", benchmark.EXACT)
        self.assertEqual(
            benchmark.SCHEDULER_STREAMS,
            "ARRAY['embedding_demo.corpus_v1', 'embedding_demo.corpus_v2', 'live_rag.differential']",
        )

    def test_periodic_full_rebuild_receives_configured_vector_dimensions(self):
        self.assertIn(":'dimensions'::integer", benchmark.MAINTENANCE["periodic_full"])

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

    def test_rag_runner_rejects_corpora_above_100k_before_connecting(self):
        result = subprocess.run(
            [sys.executable, benchmark.__file__, "--rows", "100001", "--dimensions", "384",
             "--candidate-commit", "0" * 40, "--package-sha256", "0" * 64],
            capture_output=True, text=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("larger RAG corpora are out of scope", result.stderr)

    def test_availability_reports_searchable_absent_and_stale_rows(self):
        sql = benchmark.availability_sql("SELECT 1 AS id")
        for approach in ("application", "periodic_full", "differential"):
            self.assertIn(f"'{approach}_absent'", sql)
            self.assertIn(f"'{approach}_stale'", sql)

    def test_default_changed_ids_match_availability_query(self):
        self.assertRegex(benchmark.CHANGED_IDS, r"\bAS id\s+FROM\b")
        self.assertIn(
            "JOIN changed USING (id)", benchmark.availability_sql(benchmark.CHANGED_IDS)
        )

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
