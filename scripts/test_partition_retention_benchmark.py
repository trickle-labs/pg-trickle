import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch


BENCHMARK = Path(__file__).resolve().parent.parent / "benchmarks/partition-retention/benchmark.py"
spec = importlib.util.spec_from_file_location("partition_retention_benchmark", BENCHMARK)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class InternalPlanSummaryTests(unittest.TestCase):
    def test_database_error_reports_server_diagnostic(self):
        error = subprocess.CalledProcessError(1, ["psql"], stderr="ERROR: refresh failed\n")
        with patch.object(benchmark.subprocess, "run", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "PostgreSQL command failed: ERROR: refresh failed") as raised:
                benchmark.run("db", "SELECT 1")
        self.assertTrue(raised.exception.__suppress_context__)

    def test_pending_events_rejects_negative_values(self):
        with patch("sys.argv", ["benchmark.py", "--database", "db", "--output", "out.json",
                                "--pending-events", "-1"]), patch("sys.stderr", new_callable=io.StringIO) as err:
            with self.assertRaises(SystemExit) as exit_error:
                benchmark.main()
        self.assertEqual(exit_error.exception.code, 2)
        self.assertIn("--pending-events must be nonnegative", err.getvalue())

    def test_counts_actual_partition_and_output_scans(self):
        source = {"Query Text": "source query", "Plan": {"Node Type": "Append", "Plans": [
            {"Node Type": "Seq Scan", "Schema": "trial", "Relation Name": "p1",
             "Actual Rows": 4, "Rows Removed by Filter": 1, "Actual Loops": 2},
            {"Node Type": "Seq Scan", "Schema": "trial", "Relation Name": "p2",
             "Actual Rows": 3, "Actual Loops": 1},
            {"Node Type": "Seq Scan", "Schema": "other", "Relation Name": "p3",
             "Actual Rows": 99, "Actual Loops": 1},
            {"Node Type": "Seq Scan", "Schema": "trial", "Relation Name": "p4",
             "Actual Rows": 0, "Actual Loops": 0},
        ]}}
        output = {"Query Text": "output query", "Plan": {
            "Node Type": "Bitmap Heap Scan", "Schema": "trial", "Relation Name": "totals",
            "Actual Rows": 5, "Rows Removed by Index Recheck": 2, "Actual Loops": 1,
            "Recheck Cond": "d >= boundary",
        }}
        log = "noise\nLOG: duration: 1 ms plan:\n" + json.dumps(source) + "\nLOG: duration: 2 ms plan:\n" + json.dumps(output)
        self.assertEqual(benchmark.internal_plan_summary(log, "trial"), {
            "captured_statements": 2,
            "source_partitions_scanned": ["p1", "p2"],
            "source_rows_examined": 13,
            "output_rows_examined": 7,
            "relevant_plans": [
                {"scans": [{"relation": "p2", "node": "Seq Scan", "rows_examined": 3, "condition": None},
                           {"relation": "p1", "node": "Seq Scan", "rows_examined": 10, "condition": None}], "plan": source},
                {"scans": [{"relation": "totals", "node": "Bitmap Heap Scan", "rows_examined": 7, "condition": "d >= boundary"}], "plan": output},
            ],
        })

    def test_missing_plan_is_an_error(self):
        with self.assertRaisesRegex(ValueError, "did not emit"):
            benchmark.internal_plan_summary("no plans", "trial")

    def test_partitioned_output_scan_is_included_in_guard(self):
        plan = {"Query Text": "output query", "Plan": {
            "Node Type": "Seq Scan", "Schema": "trial", "Relation Name": "totals_p0",
            "Actual Rows": 12, "Actual Loops": 1,
        }}
        summary = benchmark.internal_plan_summary("LOG: plan:\n" + json.dumps(plan), "trial")
        self.assertEqual(summary["output_rows_examined"], 12)
        with self.assertRaisesRegex(RuntimeError, "unbounded output scan"):
            benchmark.validate_trim_drop_plan(summary)

    def test_trim_plan_rejects_retained_source_and_full_output_scans(self):
        summary = {"source_partitions_scanned": ["p0"], "relevant_plans": [
            {"scans": [{"relation": "totals", "node": "Index Scan", "condition": "row_id = value"}]},
        ]}
        benchmark.validate_trim_drop_plan(summary)
        summary["source_partitions_scanned"].append("p1")
        with self.assertRaisesRegex(RuntimeError, "retained source partitions"):
            benchmark.validate_trim_drop_plan(summary)
        summary["source_partitions_scanned"].pop()
        summary["relevant_plans"][0]["scans"][0]["condition"] = None
        with self.assertRaisesRegex(RuntimeError, "unbounded output scan"):
            benchmark.validate_trim_drop_plan(summary)

    def test_baseline_requires_reinitialization_in_the_timed_backend(self):
        history = [{"action": "REINITIALIZE", "status": "COMPLETED"}]
        summary = {"source_partitions_scanned": [f"p{i}" for i in range(1, 31)]}
        benchmark.validate_baseline_refresh(history, summary)
        with self.assertRaisesRegex(RuntimeError, "did not complete a reinitialization"):
            benchmark.validate_baseline_refresh([{"action": "DIFFERENTIAL", "status": "COMPLETED"}], summary)
        with self.assertRaisesRegex(RuntimeError, "did not scan every retained partition"):
            benchmark.validate_baseline_refresh(history, {"source_partitions_scanned": []})

    def test_refresh_retries_until_the_full_oracle_matches(self):
        with patch.object(benchmark, "run", side_effect=["", "2", "", "0"]) as run, \
             patch.object(benchmark.time, "sleep"):
            self.assertEqual(benchmark.refresh_until_consistent("db", "s.t", "SELECT * FROM source"), 2)
        self.assertEqual(run.call_count, 4)


if __name__ == "__main__":
    unittest.main()
