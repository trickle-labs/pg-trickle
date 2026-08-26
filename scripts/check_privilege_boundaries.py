#!/usr/bin/env python3
"""Enforce the small, explicit privilege boundary for exported Rust APIs.

This complements the SQL API policy checker: it checks that Rust security
definers have the pinned path and do not directly accept caller SQL, assemble
dynamic SQL, or call the optional external integration layer. The checks are
deliberately source-local so a new unsafe entry point fails CI immediately.
"""

from __future__ import annotations

import re
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PINNED_PATH = "#[search_path(pgtrickle, pg_catalog, pg_temp)]"


def _definer_bodies(source: str) -> list[tuple[int, str]]:
    lines = source.splitlines()
    bodies: list[tuple[int, str]] = []
    for index, line in enumerate(lines):
        if "#[pg_extern(" not in line or "security_definer" not in line:
            continue
        if 'schema = "pgtrickle"' not in line:
            continue
        path_end = min(index + 5, len(lines))
        if not any(PINNED_PATH == re.sub(r"\s+", " ", candidate.strip()) for candidate in lines[index + 1 : path_end]):
            bodies.append((index + 1, "missing pinned search_path"))
            continue

        fn_index = next(
            (i for i in range(index + 1, min(index + 10, len(lines))) if re.search(r"\bfn\s+\w+", lines[i])),
            None,
        )
        if fn_index is None:
            bodies.append((index + 1, "security definer has no function body"))
            continue
        depth = 0
        body_lines: list[str] = []
        for body_index in range(fn_index, len(lines)):
            body_lines.append(lines[body_index])
            depth += lines[body_index].count("{") - lines[body_index].count("}")
            if body_index > fn_index and depth == 0:
                break
        bodies.append((index + 1, "\n".join(body_lines)))
    return bodies


def check_rust_definer_paths(source: str) -> list[str]:
    return [f"line {line}: {body}" for line, body in _definer_bodies(source) if body == "missing pinned search_path"]


def check_caller_sql(source: str) -> list[str]:
    return [
        f"line {line}: caller-controlled SQL reaches a SECURITY DEFINER body"
        for line, body in _definer_bodies(source)
        if body != "missing pinned search_path"
        and re.search(r"\bSpi::run\s*\(\s*(?:&\s*)?(?:sql|query)\b", body)
    ]


def check_dynamic_identifiers(source: str) -> list[str]:
    return [
        f"line {line}: dynamic SQL is assembled directly in a SECURITY DEFINER body"
        for line, body in _definer_bodies(source)
        if body != "missing pinned search_path"
        and re.search(r"\bSpi::run\s*\(\s*&?format!\s*\(", body)
    ]


def check_external_calls(source: str) -> list[str]:
    return [
        f"line {line}: external extension call reaches a SECURITY DEFINER body"
        for line, body in _definer_bodies(source)
        if body != "missing pinned search_path"
        and re.search(r"\b(?:pg_tide|tide::|extension_call)\b", body)
    ]


def check_sql_definer_paths(source: str) -> list[str]:
    violations = []
    for match in re.finditer(r"SECURITY\s+DEFINER\b", source, re.IGNORECASE):
        tail = source[match.end() : match.end() + 400]
        next_definer = re.search(r"SECURITY\s+DEFINER\b", tail, re.IGNORECASE)
        search_path = re.search(
            r"SET\s+search_path\s+(?:TO|=)\s+pgtrickle\s*,\s*pg_catalog\s*,\s*pg_temp",
            tail,
            re.IGNORECASE,
        )
        if search_path is None or (next_definer and next_definer.start() < search_path.start()):
            violations.append("SQL SECURITY DEFINER statement lacks the pinned search_path")
        elif re.search(r"SET\s+search_path[^;]*\bpublic\b", tail[: search_path.end()], re.IGNORECASE):
            violations.append("SQL SECURITY DEFINER statement uses public in search_path")
    return violations


def run_checks() -> list[str]:
    violations: list[str] = []
    for path in sorted((ROOT / "src").rglob("*.rs")):
        source = path.read_text(encoding="utf-8")
        for check in (check_rust_definer_paths, check_caller_sql, check_dynamic_identifiers, check_external_calls):
            violations.extend(f"{path}: {item}" for item in check(source))
    version = re.search(
        r'^version\s*=\s*"([^"]+)"',
        (ROOT / "Cargo.toml").read_text(encoding="utf-8"),
        re.MULTILINE,
    ).group(1)
    sql_paths = [ROOT / "sql" / "archive" / f"pg_trickle--{version}.sql"]
    sql_paths.extend((ROOT / "sql").glob(f"pg_trickle--*--{version}.sql"))
    for path in sorted(sql_paths):
        violations.extend(f"{path}: {item}" for item in check_sql_definer_paths(path.read_text(encoding="utf-8")))

    policy_check = subprocess.run(
        [sys.executable, str(ROOT / "scripts/check_sql_api_policy.py"), "check"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if policy_check.returncode:
        violations.append("SQL API policy coverage failed:\n" + policy_check.stdout + policy_check.stderr)
    return violations


class BoundarySelfTests(unittest.TestCase):
    def test_pinned_path_rule(self) -> None:
        good = '#[pg_extern(schema = "pgtrickle", security_definer)]\n#[search_path(pgtrickle, pg_catalog, pg_temp)]\nfn ok() {}'
        bad = '#[pg_extern(schema = "pgtrickle", security_definer)]\n#[search_path(public)]\nfn bad() {}'
        self.assertEqual(check_rust_definer_paths(good), [])
        self.assertTrue(check_rust_definer_paths(bad))

    def test_caller_sql_rule(self) -> None:
        bad = '#[pg_extern(schema = "pgtrickle", security_definer)]\n#[search_path(pgtrickle, pg_catalog, pg_temp)]\nfn bad(sql: &str) { Spi::run(sql); }'
        good = '#[pg_extern(schema = "pgtrickle")]\nfn ok(sql: &str) { Spi::run(sql); }'
        self.assertTrue(check_caller_sql(bad))
        self.assertEqual(check_caller_sql(good), [])

    def test_dynamic_identifier_rule(self) -> None:
        source = '#[pg_extern(schema = "pgtrickle", security_definer)]\n#[search_path(pgtrickle, pg_catalog, pg_temp)]\nfn bad() { Spi::run(&format!("DROP TABLE {}", "x")); }'
        self.assertTrue(check_dynamic_identifiers(source))

    def test_external_call_rule(self) -> None:
        source = '#[pg_extern(schema = "pgtrickle", security_definer)]\n#[search_path(pgtrickle, pg_catalog, pg_temp)]\nfn bad() { pg_tide::attach(); }'
        self.assertTrue(check_external_calls(source))

    def test_sql_pinned_path_rule(self) -> None:
        good = "CREATE FUNCTION x() SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp;"
        bad = "CREATE FUNCTION x() SECURITY DEFINER SET search_path TO public;"
        self.assertEqual(check_sql_definer_paths(good), [])
        self.assertTrue(check_sql_definer_paths(bad))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "self-test":
        sys.argv.pop(1)
        unittest.main()
    errors = run_checks()
    if errors:
        print("check_privilege_boundaries: FAILED")
        print("\n".join(errors))
        raise SystemExit(1)
    print("check_privilege_boundaries: all checks passed")
