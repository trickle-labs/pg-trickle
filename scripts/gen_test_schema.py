#!/usr/bin/env python3
"""
gen_test_schema.py — Extract catalog DDL from the latest archive SQL and
emit a Rust raw-string-literal file for tests/generated/schema.rs.

The generated file is included in tests/common/mod.rs via include!().
It contains the exact table definitions (including correct CHECK constraint
values) and views taken directly from the authoritative archive SQL, plus a
PL/pgSQL stub for parse_duration_seconds (the extension uses MODULE_PATHNAME
which cannot be loaded in a stock test container).

Usage:
    python3 scripts/gen_test_schema.py > tests/generated/schema.rs
    python3 scripts/gen_test_schema.py --check   # exits 1 if drift detected
"""

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Tables to extract from the archive SQL (order matters for FK dependencies).
TABLES = [
    "pgt_stream_tables",
    "pgt_dependencies",
    "pgt_refresh_history",
    "pgt_change_tracking",
    "pgt_scheduler_jobs",
    "pgt_ducklake_provenance",
]

# Views to extract from the archive SQL (must come after tables they reference).
VIEWS = [
    "stream_tables_info",
    "pg_stat_stream_tables",
    "quick_health",
]

# PL/pgSQL stub for parse_duration_seconds.
# The extension defines this via MODULE_PATHNAME (Rust), but test containers
# run a stock postgres image with no extension library loaded.  This pure-SQL
# implementation is functionally identical for the supported format strings.
_PARSE_DURATION_STUB = """\
CREATE OR REPLACE FUNCTION pgtrickle.parse_duration_seconds(input TEXT)
RETURNS BIGINT LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    s TEXT := trim(input);
    total BIGINT := 0;
    num TEXT := '';
    c CHAR;
BEGIN
    IF s IS NULL OR s = '' THEN RETURN NULL; END IF;
    FOR i IN 1..length(s) LOOP
        c := substr(s, i, 1);
        IF c BETWEEN '0' AND '9' THEN
            num := num || c;
        ELSIF c = 'h' THEN
            total := total + num::bigint * 3600; num := '';
        ELSIF c = 'm' THEN
            total := total + num::bigint * 60; num := '';
        ELSIF c = 's' THEN
            total := total + num::bigint; num := '';
        ELSIF c = 'd' THEN
            total := total + num::bigint * 86400; num := '';
        END IF;
    END LOOP;
    IF num <> '' THEN total := total + num::bigint; END IF;
    RETURN total;
END; $$;"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_archive_sql() -> Path:
    """Return the path to the latest pg_trickle archive SQL file."""
    archive_dir = REPO_ROOT / "sql" / "archive"
    version_re = re.compile(r"pg_trickle--(\d+)\.(\d+)\.(\d+)\.sql$")
    candidates: list[tuple[tuple[int, int, int], Path]] = []
    for f in archive_dir.glob("pg_trickle--*.sql"):
        m = version_re.match(f.name)
        if m:
            v = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
            candidates.append((v, f))
    if not candidates:
        print("ERROR: no archive SQL found in sql/archive/", file=sys.stderr)
        sys.exit(1)
    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]


def _strip_comments(sql: str) -> str:
    """Remove -- line comments and /* */ block comments from SQL text."""
    # Block comments first (may span lines)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # Line comments
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


def _extract_create_table(sql_text: str, table_name: str) -> str | None:
    """
    Extract the CREATE TABLE ... ; statement for table_name (in pgtrickle schema).
    Handles nested parentheses inside column definitions.
    """
    pat = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?pgtrickle\." + re.escape(table_name) + r"\b",
        re.IGNORECASE,
    )
    m = pat.search(sql_text)
    if not m:
        return None

    start = m.start()
    i = start
    depth = 0
    in_sq = False  # inside single-quoted string
    n = len(sql_text)

    while i < n:
        c = sql_text[i]
        if in_sq:
            if c == "'" and i + 1 < n and sql_text[i + 1] == "'":
                i += 2  # escaped quote
                continue
            if c == "'":
                in_sq = False
        else:
            if c == "'":
                in_sq = True
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    # Find the semicolon that follows
                    end = sql_text.find(";", i)
                    if end != -1:
                        return sql_text[start : end + 1].strip()
                    return None
        i += 1
    return None


def _extract_indexes_for_table(sql_text: str, table_name: str) -> list[str]:
    """Return all CREATE INDEX … ON pgtrickle.<table_name> statements."""
    pat = re.compile(
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+\s+ON\s+pgtrickle\."
        + re.escape(table_name)
        + r"\s*\(",
        re.IGNORECASE,
    )
    results = []
    for m in pat.finditer(sql_text):
        start = m.start()
        end = sql_text.find(";", start)
        if end != -1:
            results.append(sql_text[start : end + 1].strip())
    return results


def _extract_create_view(sql_text: str, view_name: str) -> str | None:
    """
    Extract the CREATE [OR REPLACE] VIEW pgtrickle.<view_name> AS ... ; statement.
    Tracks parenthesis depth to handle subqueries.
    """
    pat = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+pgtrickle\." + re.escape(view_name) + r"\b",
        re.IGNORECASE,
    )
    m = pat.search(sql_text)
    if not m:
        return None

    start = m.start()
    i = start
    depth = 0
    in_sq = False
    started_body = False
    n = len(sql_text)

    # Find the AS keyword to know when the body begins
    as_match = re.search(r"\bAS\b", sql_text[start:], re.IGNORECASE)
    if not as_match:
        return None
    body_start = start + as_match.end()

    i = body_start
    while i < n:
        c = sql_text[i]
        if in_sq:
            if c == "'" and i + 1 < n and sql_text[i + 1] == "'":
                i += 2
                continue
            if c == "'":
                in_sq = False
        else:
            if c == "'":
                in_sq = True
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == ";" and depth == 0:
                return sql_text[start : i + 1].strip()
        i += 1
    return None


# ---------------------------------------------------------------------------
# Main generation logic
# ---------------------------------------------------------------------------

def generate(archive_sql: str) -> str:
    """
    Generate the full test schema DDL from the archive SQL.
    Returns a string that can be wrapped in a Rust raw string literal.
    """
    clean_sql = _strip_comments(archive_sql)
    # Restore structure needed for extraction (strip_comments may collapse spaces)
    # We work on clean_sql for pattern matching, original for content extraction.

    parts: list[str] = []

    # 1. Schema creation
    parts.append("CREATE SCHEMA IF NOT EXISTS pgtrickle;")
    parts.append("CREATE SCHEMA IF NOT EXISTS pgtrickle_changes;\n")

    # 2. Tables and their indexes
    for table in TABLES:
        stmt = _extract_create_table(clean_sql, table)
        if stmt is None:
            print(f"WARNING: table {table!r} not found in archive SQL", file=sys.stderr)
        else:
            parts.append(stmt)
            parts.append("")
        indexes = _extract_indexes_for_table(clean_sql, table)
        for idx in indexes:
            parts.append(idx)
        if indexes:
            parts.append("")

    # 3. PL/pgSQL stub for parse_duration_seconds (must precede views that call it)
    parts.append(_PARSE_DURATION_STUB)
    parts.append("")

    # 4. Views
    for view in VIEWS:
        stmt = _extract_create_view(clean_sql, view)
        if stmt is None:
            print(f"WARNING: view {view!r} not found in archive SQL", file=sys.stderr)
        else:
            parts.append(stmt)
            parts.append("")

    return "\n".join(parts).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if tests/generated/schema.rs does not match current output",
    )
    args = parser.parse_args()

    archive_path = _find_archive_sql()
    archive_sql = archive_path.read_text(encoding="utf-8")

    ddl = generate(archive_sql)

    # Wrap in a Rust raw string literal (use r#"..."# — the DDL will not contain "#)
    if '"#' in ddl:
        print(
            "ERROR: generated DDL contains '\"#' which would break the Rust raw string. "
            "Use a longer delimiter.",
            file=sys.stderr,
        )
        sys.exit(1)
    output = f'r#"\n{ddl}"#\n'

    generated_path = REPO_ROOT / "tests" / "generated" / "schema.rs"

    if args.check:
        if not generated_path.exists():
            print(
                f"ERROR: {generated_path} does not exist.\n"
                "Run: python3 scripts/gen_test_schema.py > tests/generated/schema.rs",
                file=sys.stderr,
            )
            sys.exit(1)
        existing = generated_path.read_text(encoding="utf-8")
        if existing != output:
            print(
                "ERROR: tests/generated/schema.rs is out of date with the archive SQL.\n"
                "Run: python3 scripts/gen_test_schema.py > tests/generated/schema.rs",
                file=sys.stderr,
            )
            sys.exit(1)
        print("OK: tests/generated/schema.rs is up to date.")
    else:
        sys.stdout.write(output)


if __name__ == "__main__":
    main()
