#!/usr/bin/env python3
"""DOC-1 (v0.80.0): Docs lint — compare #[pg_extern] exports with SQL docs.

Extracts every SQL function exported via `#[pg_extern(schema = "pgtrickle" ...)]`
from Rust source files and checks that each appears in EITHER:
  - docs/SQL_REFERENCE.md  (user-facing narrative reference), OR
  - docs/SQL_API_CATALOG.md  (comprehensive auto-generated catalog)

Functions that appear in neither doc are flagged as undocumented.
Truly internal functions (e.g. underscore-prefixed, IVM trigger plumbing)
must be listed in ALLOWED_MISSING.

Exits non-zero if any exported function is missing from both docs.
"""

import re
import sys
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = WORKSPACE_ROOT / "src"
SQL_REFERENCE = WORKSPACE_ROOT / "docs" / "SQL_REFERENCE.md"
SQL_API_CATALOG = WORKSPACE_ROOT / "docs" / "SQL_API_CATALOG.md"

# Attribute pattern: matches `#[pg_extern(schema = "pgtrickle" ...)]` including
# multi-line attributes, or the simpler `#[pg_extern(schema = "pgtrickle")]`.
PG_EXTERN_RE = re.compile(
    r'#\[pg_extern\s*\([^)]*schema\s*=\s*"pgtrickle"[^)]*\)\]',
    re.DOTALL,
)
# Explicit name override: `name = "some_name"`
NAME_OVERRIDE_RE = re.compile(r'name\s*=\s*"([^"]+)"')
# Function name following the attribute block
FN_NAME_RE = re.compile(r'\bfn\s+([a-z_][a-z0-9_]*)\s*[\(<]')

# Functions intentionally not documented in SQL_REFERENCE.md or SQL_API_CATALOG.md.
# These are internal/plumbing functions exposed as pg_extern for PostgreSQL
# callback registration (event triggers, IVM trigger plumbing, etc.).
ALLOWED_MISSING: set[str] = {
    # Event trigger callbacks — registered as PostgreSQL event triggers,
    # not intended to be called directly by users.
    "_on_ddl_end",
    "_on_sql_drop",
    # Internal launcher rescan signal — called by the background worker.
    "_signal_launcher_rescan",
    # IVM trigger plumbing — called by PostgreSQL trigger machinery,
    # not user-callable.
    "pgt_ivm_apply_delta",
    "pgt_ivm_apply_delta_enr",
    "pgt_ivm_handle_truncate",
    # Internal Citus helpers.
    "handle_vp_promoted",
    "source_stable_name",
    # Internal diagnostic / migration helpers.
    "clear_caches",
    "migrate",
    "parse_duration_seconds",
}


def extract_pg_extern_names(src_dir: Path) -> list[tuple[str, str]]:
    """Return list of (function_sql_name, source_file) tuples."""
    results: list[tuple[str, str]] = []
    for rs_file in sorted(src_dir.rglob("*.rs")):
        text = rs_file.read_text(encoding="utf-8", errors="replace")
        for m in PG_EXTERN_RE.finditer(text):
            attr_text = m.group(0)
            name_m = NAME_OVERRIDE_RE.search(attr_text)
            if name_m:
                sql_name = name_m.group(1)
            else:
                remainder = text[m.end():]
                fn_m = FN_NAME_RE.search(remainder[:300])
                if not fn_m:
                    continue
                sql_name = fn_m.group(1)
            rel = str(rs_file.relative_to(WORKSPACE_ROOT))
            results.append((sql_name, rel))
    return results


def load_names_from_doc(path: Path) -> set[str]:
    """Return set of SQL function names mentioned in a Markdown doc."""
    if not path.exists():
        return set()
    text = path.read_text(encoding="utf-8", errors="replace")
    return set(re.findall(r'pgtrickle\.([a-z_][a-z0-9_]*)', text))


def main() -> int:
    if not SQL_REFERENCE.exists():
        print(f"ERROR: {SQL_REFERENCE} not found", file=sys.stderr)
        return 1

    exported = extract_pg_extern_names(SRC_DIR)
    # Union of both docs — a function documented in either is considered covered.
    documented = load_names_from_doc(SQL_REFERENCE) | load_names_from_doc(SQL_API_CATALOG)

    missing: list[tuple[str, str]] = []
    seen: set[str] = set()
    for fn_name, src_file in exported:
        if fn_name in seen:
            continue
        seen.add(fn_name)
        if fn_name not in ALLOWED_MISSING and fn_name not in documented:
            missing.append((fn_name, src_file))

    if missing:
        print("DOC-1 FAILED: the following #[pg_extern] functions are missing from")
        print(f"  both {SQL_REFERENCE.relative_to(WORKSPACE_ROOT)}")
        print(f"  and  {SQL_API_CATALOG.relative_to(WORKSPACE_ROOT)}:")
        for fn_name, src_file in sorted(missing):
            print(f"  - pgtrickle.{fn_name}  (defined in {src_file})")
        print()
        print("Add a documentation entry for each function or add it to")
        print("ALLOWED_MISSING in scripts/check_pg_extern_docs.py if it is")
        print("intentionally undocumented (e.g. an internal helper).")
        return 1

    print(
        f"DOC-1 passed: {len(seen)} exported function(s) covered by SQL docs "
        f"({len(load_names_from_doc(SQL_REFERENCE))} in SQL_REFERENCE.md, "
        f"{len(load_names_from_doc(SQL_API_CATALOG))} in SQL_API_CATALOG.md)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

