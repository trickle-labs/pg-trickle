import importlib.util
import json
from pathlib import Path
import unittest


BENCHMARK = Path(__file__).resolve().parent.parent / "benchmarks/partition-retention/benchmark.py"
spec = importlib.util.spec_from_file_location("partition_retention_benchmark", BENCHMARK)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class InternalPlanSummaryTests(unittest.TestCase):
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
        }}
        log = "noise\nLOG: duration: 1 ms plan:\n" + json.dumps(source) + "\nLOG: duration: 2 ms plan:\n" + json.dumps(output)
        self.assertEqual(benchmark.internal_plan_summary(log, "trial"), {
            "captured_statements": 2,
            "source_partitions_scanned": ["p1", "p2"],
            "source_rows_examined": 13,
            "output_rows_examined": 7,
            "relevant_plans": [
                {"scans": [{"relation": "p2", "node": "Seq Scan", "rows_examined": 3},
                           {"relation": "p1", "node": "Seq Scan", "rows_examined": 10}], "plan": source},
                {"scans": [{"relation": "totals", "node": "Bitmap Heap Scan", "rows_examined": 7}], "plan": output},
            ],
        })

    def test_missing_plan_is_an_error(self):
        with self.assertRaisesRegex(ValueError, "did not emit"):
            benchmark.internal_plan_summary("no plans", "trial")

    def test_trim_plan_rejects_retained_source_and_full_output_scans(self):
        summary = {"source_partitions_scanned": ["p0"], "relevant_plans": [
            {"scans": [{"relation": "totals", "node": "Index Scan"}]},
        ]}
        benchmark.validate_trim_drop_plan(summary)
        summary["source_partitions_scanned"].append("p1")
        with self.assertRaisesRegex(RuntimeError, "retained source partitions"):
            benchmark.validate_trim_drop_plan(summary)
        summary["source_partitions_scanned"].pop()
        summary["relevant_plans"][0]["scans"][0]["node"] = "Seq Scan"
        with self.assertRaisesRegex(RuntimeError, "entire output table"):
            benchmark.validate_trim_drop_plan(summary)


if __name__ == "__main__":
    unittest.main()
