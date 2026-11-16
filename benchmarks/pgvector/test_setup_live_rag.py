import unittest

import setup_live_rag


class SetupTests(unittest.TestCase):
    def test_setup_rewrites_both_fixtures_atomically(self):
        sql = setup_live_rag.setup_sql(384)
        self.assertTrue(sql.startswith("BEGIN;\n"))
        self.assertTrue(sql.endswith("\nCOMMIT;\n"))
        self.assertNotIn("vector(3)", sql)
        self.assertIn("embedding vector(384)", sql)
        self.assertIn("embedding::vector(384)", sql)
        self.assertNotIn("application_ann", sql)
        self.assertNotIn("application_fts", sql)

    def test_index_setup_rewrites_dimensions_after_bulk_population(self):
        sql = setup_live_rag.index_sql(768)
        self.assertEqual(sql.count("CREATE INDEX"), 6)
        self.assertIn("embedding::vector(768)", sql)
        self.assertNotIn("vector(3)", sql)

    def test_setup_rejects_invalid_dimensions(self):
        for dimensions in (0, 1537):
            with self.assertRaises(ValueError):
                setup_live_rag.setup_sql(dimensions)
            with self.assertRaises(ValueError):
                setup_live_rag.index_sql(dimensions)


if __name__ == "__main__":
    unittest.main()
