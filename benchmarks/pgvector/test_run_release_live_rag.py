import unittest

import run_release_live_rag


class ReleaseLiveRagTests(unittest.TestCase):
    def test_release_live_rag_supports_10k_100k_and_all_dimensions_but_not_1m(self):
        parsed = run_release_live_rag.parser().parse_args(
            ["--rows", "10000", "100000", "--dimensions", "384", "768", "1536"]
        )
        self.assertEqual(tuple(parsed.rows), (10_000, 100_000))
        self.assertEqual(tuple(parsed.dimensions), (384, 768, 1536))
        with self.assertRaises(SystemExit):
            run_release_live_rag.parser().parse_args(
                ["--rows", "1000000", "--dimensions", "384", "768", "1536"]
            )


if __name__ == "__main__":
    unittest.main()
