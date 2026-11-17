import unittest

import run_release_live_rag


class ReleaseLiveRagTests(unittest.TestCase):
    def test_release_live_rag_is_capped_to_100k_and_supported_high_dimensions(self):
        parsed = run_release_live_rag.parser().parse_args(
            ["--rows", "100000", "--dimensions", "768", "1536"]
        )
        self.assertEqual(parsed.rows, 100_000)
        self.assertEqual(tuple(parsed.dimensions), (768, 1536))
        with self.assertRaises(SystemExit):
            run_release_live_rag.parser().parse_args(
                ["--rows", "1000000", "--dimensions", "768", "1536"]
            )


if __name__ == "__main__":
    unittest.main()
