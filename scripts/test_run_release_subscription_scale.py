import gzip
import json
import tempfile
import unittest

import run_release_subscription_scale as scale


def report(
    rows: int,
    subscriptions: int,
    analyzed: bool,
    dimensions: int = 384,
    pivots: int = 1,
    force_explicit: bool = True,
) -> dict:
    return {
        "pg_trickle": "0.109.0",
        "pgvector": "0.8.7",
        "postgres": "PostgreSQL 18.6 on x86_64-pc-linux-gnu",
        "dimensions": dimensions,
        "distinct_pivots": pivots,
        "force_explicit_with_noop_trigger": force_explicit,
        "nested_plan_capture": False,
        "measurements": [
            {
                "initial_rows": rows,
                "subscriptions": subscriptions,
                "analyzed": analyzed,
                "samples": [
                    {
                        "merge_strategy": "explicit_dml" if force_explicit else "delete_insert",
                        "refresh_ms": 1.0,
                        "commit_ms": 0.1,
                        "stream_sequential_tuples_returned": 0,
                        "stream_index_tuples_fetched": 1,
                        "source_sequential_tuples_returned": 0,
                        "source_index_tuples_fetched": 1,
                    }
                ],
            }
        ],
    }


class SubscriptionScaleTests(unittest.TestCase):
    def test_parse_case_accepts_the_requested_measurement(self):
        data = report(1_000_000, 10, True)
        output = scale.REPORT_MARKER + json.dumps(data)
        self.assertEqual(scale.parse_case(output, 1_000_000, 10, True), data)

    def test_parse_case_accepts_trigger_free_768_dimension_measurement(self):
        data = report(1_000_000, 1_000, True, dimensions=768, pivots=10, force_explicit=False)
        output = scale.REPORT_MARKER + json.dumps(data)
        self.assertEqual(
            scale.parse_case(
                output,
                1_000_000,
                1_000,
                True,
                dimensions=768,
                force_explicit=False,
                pivots=10,
            ),
            data,
        )

    def test_parse_case_rejects_wrong_scale_or_uncontrolled_merge(self):
        wrong_scale = report(1_000_000, 10, True)
        wrong_scale["measurements"][0]["initial_rows"] -= 1
        with self.assertRaisesRegex(ValueError, "does not match"):
            scale.parse_case(scale.REPORT_MARKER + json.dumps(wrong_scale), 1_000_000, 10, True)

        wrong_strategy = report(1_000_000, 10, True)
        wrong_strategy["measurements"][0]["samples"][0]["merge_strategy"] = "delete_insert"
        with self.assertRaisesRegex(ValueError, "controlled merge strategy"):
            scale.parse_case(scale.REPORT_MARKER + json.dumps(wrong_strategy), 1_000_000, 10, True)

    def test_parse_case_rejects_missing_tuple_counters(self):
        missing_counter = report(1_000, 10, False)
        del missing_counter["measurements"][0]["samples"][0]["source_index_tuples_fetched"]
        with self.assertRaisesRegex(ValueError, "source_index_tuples_fetched"):
            scale.parse_case(scale.REPORT_MARKER + json.dumps(missing_counter), 1_000, 10, False)

    def test_matrix_includes_the_one_million_source_row_limit(self):
        self.assertEqual(scale.ROW_COUNTS, (1_000, 10_000, 100_000, 1_000_000))
        self.assertEqual(scale.SUBSCRIPTIONS, (0, 1, 10, 100, 1_000))
        self.assertEqual(scale.DIMENSIONS, (384, 768))
        self.assertEqual(
            scale.CASE_GROUPS,
            (
                (384, scale.ROW_COUNTS, scale.STATISTICS, True, scale.SUBSCRIPTIONS),
                (384, (1_000_000,), ("analyzed",), False, (0, 1_000)),
                (768, (1_000_000,), ("analyzed",), False, (0, 1_000)),
            ),
        )
        self.assertEqual(
            sum(
                len(rows) * len(statistics) * len(subscriptions)
                for _, rows, statistics, _, subscriptions in scale.CASE_GROUPS
            ),
            44,
        )

    def test_matrix_captures_same_strategy_plans_at_zero_and_max_subscriptions(self):
        expected = {
            (384, 1_000_000, 0, "unanalyzed", True),
            (384, 1_000_000, 1_000, "unanalyzed", True),
            (384, 1_000_000, 0, "analyzed", False),
            (384, 1_000_000, 1_000, "analyzed", False),
            (768, 1_000_000, 0, "analyzed", False),
            (768, 1_000_000, 1_000, "analyzed", False),
        }
        self.assertEqual(scale.PLAN_CASES, expected)
        self.assertTrue(scale.PLAN_CASES <= {
            (dimensions, rows, subscriptions, statistics, force_explicit)
            for dimensions, row_counts, statistics_values, force_explicit, subscriptions_values in scale.CASE_GROUPS
            for rows in row_counts
            for subscriptions in subscriptions_values
            for statistics in statistics_values
        })

    def test_parse_case_requires_the_declared_plan_capture(self):
        data = report(1_000_000, 10, False)
        data["nested_plan_capture"] = True
        output = scale.REPORT_MARKER + json.dumps(data)
        self.assertEqual(
            scale.parse_case(output, 1_000_000, 10, False, plan_capture=True), data
        )
        with self.assertRaisesRegex(ValueError, "plan-control"):
            scale.parse_case(output, 1_000_000, 10, False)

    def test_plan_log_requires_case_markers_and_an_explain_plan(self):
        case = "rows=1000000 subscriptions=0 analyzed=false round=0"
        plan = (
            f"2026-11-18 18:11:32 UTC [1975] LOG:  PGVECTOR_SUB_START {case}\n"
            f"2026-11-18 18:11:32 UTC [1975] STATEMENT: DO $marker$ BEGIN RAISE LOG 'PGVECTOR_SUB_START {case}'; END; $marker$\n"
            '{"Plan":{"Node Type":"Seq Scan"}}\n'
            f"2026-11-18 18:11:39 UTC [1975] LOG:  PGVECTOR_SUB_END {case}\n"
            f"2026-11-18 18:11:39 UTC [1975] STATEMENT: DO $marker$ BEGIN RAISE LOG 'PGVECTOR_SUB_END {case}'; END; $marker$\n"
        )
        scale.validate_plan_log(plan, 1_000_000, 0, "unanalyzed")
        with self.assertRaisesRegex(ValueError, "case markers"):
            scale.validate_plan_log(plan.replace("PGVECTOR_SUB_END", "OTHER"), 1_000_000, 0, "unanalyzed")
        duplicate_start = plan.replace(
            f"LOG:  PGVECTOR_SUB_START {case}\n",
            f"LOG:  PGVECTOR_SUB_START {case}\nLOG:  PGVECTOR_SUB_START {case}\n",
        )
        with self.assertRaisesRegex(ValueError, "case markers"):
            scale.validate_plan_log(duplicate_start, 1_000_000, 0, "unanalyzed")
        with self.assertRaisesRegex(ValueError, "no auto_explain plan"):
            scale.validate_plan_log(
                plan.replace('{"Plan":{"Node Type":"Seq Scan"}}\n', ""),
                1_000_000,
                0,
                "unanalyzed",
            )

    def test_partial_plan_log_can_be_archived_without_success_markers(self):
        partial = "PGVECTOR_SUB_START rows=1000000 subscriptions=1000 analyzed=false round=0\npartial"
        with tempfile.TemporaryDirectory(dir=scale.ROOT) as directory:
            path = scale.Path(directory) / "partial.log"
            path.write_text(partial, encoding="utf-8")
            result = {"raw_evidence_files": []}
            archived = scale.archive_plan_log(path, result)
            with gzip.open(scale.ROOT / archived, "rt", encoding="utf-8") as compressed:
                self.assertEqual(compressed.read(), partial)
            self.assertEqual(result["raw_evidence_files"], [archived])


if __name__ == "__main__":
    unittest.main()
