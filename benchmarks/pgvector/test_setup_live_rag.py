import unittest
from pathlib import Path

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

    def test_periodic_full_bulk_load_rebuilds_search_indexes(self):
        path = Path(setup_live_rag.__file__).with_name("live_rag_baselines.sql")
        source = path.read_text(encoding="utf-8")
        refresh = source.split("CREATE FUNCTION live_rag.recompute_full", 1)[1].split("END $$;", 1)[0]
        insert = refresh.index("INSERT INTO live_rag.periodic_full")
        for statement in (
            "DROP INDEX live_rag.periodic_full_ann",
            "DROP INDEX live_rag.periodic_full_fts",
            "CREATE INDEX periodic_full_ann",
            "CREATE INDEX periodic_full_fts",
            "ANALYZE live_rag.periodic_full",
        ):
            self.assertIn(statement, refresh)
        self.assertLess(refresh.index("TRUNCATE live_rag.periodic_full"), insert)
        self.assertGreater(refresh.index("CREATE INDEX periodic_full_ann"), insert)
        self.assertGreater(refresh.index("CREATE INDEX periodic_full_fts"), insert)

    def test_setup_rejects_invalid_dimensions(self):
        for dimensions in (0, 1537):
            with self.assertRaises(ValueError):
                setup_live_rag.setup_sql(dimensions)
            with self.assertRaises(ValueError):
                setup_live_rag.index_sql(dimensions)


if __name__ == "__main__":
    unittest.main()
