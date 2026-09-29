#!/usr/bin/env python3
"""Regression coverage for feature-gated SQL test hooks in public API docs."""

import tempfile
import unittest
from pathlib import Path

import gen_catalogs
import gen_sql_reference


TEST_ONLY_FUNCTION = "e2e_catch_security_context_error"


class ApiDocFilterTests(unittest.TestCase):
    def test_asan_probe_is_not_cataloged_or_counted_as_public_api(self) -> None:
        source_functions = gen_catalogs.extract_sql_functions(gen_catalogs.SRC_DIR)
        self.assertNotIn(TEST_ONLY_FUNCTION, {f["fn_name"] for f in source_functions})

        discovered = gen_sql_reference.discover_pg_extern_names(gen_sql_reference.SRC_DIR)
        self.assertIn(TEST_ONLY_FUNCTION, discovered)
        self.assertIn(TEST_ONLY_FUNCTION, gen_sql_reference.KNOWN_INTERNAL)

        sql = (
            "CREATE FUNCTION pgtrickle.e2e_catch_security_context_error(text, text) "
            "RETURNS text LANGUAGE c;\n"
            "CREATE FUNCTION pgtrickle.version() RETURNS text LANGUAGE c;\n"
        )
        with tempfile.TemporaryDirectory(dir=gen_catalogs.REPO_ROOT) as directory:
            sql_path = Path(directory) / "pg_trickle.sql"
            sql_path.write_text(sql, encoding="utf-8")
            pgrx_functions = gen_catalogs.extract_sql_functions_from_pgrx_sql(sql_path)

        pgrx_names = {f["fn_name"] for f in pgrx_functions}
        self.assertNotIn(TEST_ONLY_FUNCTION, pgrx_names)
        self.assertIn("version", pgrx_names)


if __name__ == "__main__":
    unittest.main()
