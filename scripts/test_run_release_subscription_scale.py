import json
import unittest

import run_release_subscription_scale as scale


def report(rows: int, subscriptions: int, analyzed: bool) -> dict:
    return {
        "pg_trickle": "0.109.0",
        "pgvector": "0.8.7",
        "postgres": "PostgreSQL 18.6 on x86_64-pc-linux-gnu",
        "dimensions": 384,
        "distinct_pivots": 1,
        "force_explicit_with_noop_trigger": True,
        "measurements": [
            {
                "initial_rows": rows,
                "subscriptions": subscriptions,
                "analyzed": analyzed,
                "samples": [
                    {
                        "merge_strategy": "explicit_dml",
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
        self.assertEqual(scale.DIMENSIONS, 384)
        self.assertEqual(len(scale.ROW_COUNTS) * len(scale.SUBSCRIPTIONS) * len(scale.STATISTICS), 40)


if __name__ == "__main__":
    unittest.main()
