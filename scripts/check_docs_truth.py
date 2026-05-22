#!/usr/bin/env python3
"""check_docs_truth.py — Docs source-surface drift gate for pg_trickle.

Scans Markdown documentation for:
  1. pg_trickle.<name> GUC references that do not appear in the generated
     GUC catalog (docs/GUC_CATALOG.md).
  2. pgtrickle.<function>( SQL function calls that do not appear in the
     generated SQL API catalog (docs/SQL_API_CATALOG.md).
  3. Removed pg_trickle-owned outbox/inbox APIs mentioned as active (outside
     negation or historical contexts).
  4. Stale default-wording: "parallel_refresh_mode = 'off'" as the default.
  5. Stale CDC default: "row-level triggers by default" / "row-level AFTER
     triggers (the default)".

Usage:
  python3 scripts/check_docs_truth.py           # report and exit non-zero on issues
  python3 scripts/check_docs_truth.py --fix      # (reserved, not implemented)

Exit codes:
  0 — no issues found
  1 — issues found or missing catalog files
"""

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"
GUC_CATALOG_PATH = DOCS_DIR / "GUC_CATALOG.md"
SQL_CATALOG_PATH = DOCS_DIR / "SQL_API_CATALOG.md"
ALLOWLIST_PATH = REPO_ROOT / "scripts" / "check_docs_truth_allowlist.yml"

# ---------------------------------------------------------------------------
# APIs removed from pg_trickle and moved to pg_tide (v0.46.0).
# References outside of a negation/historical context are flagged.
# ---------------------------------------------------------------------------
REMOVED_PGTRICKLE_APIS = [
    "enable_outbox",
    "disable_outbox",
    "poll_outbox",
    "commit_offset",
    "create_consumer_group",
    "create_inbox",
    "drop_inbox",
    "enable_inbox",
    "disable_inbox",
    "inbox_status",
    "enable_inbox_ordering",
    "inbox_is_my_partition",
]

# GUC prefixes that no longer exist in pg_trickle
REMOVED_GUC_PREFIXES = [
    "pg_trickle.outbox_",
    "pg_trickle.inbox_",
    "pg_trickle.consumer_",
]

# Stale default phrases that indicate incorrect documentation
STALE_DEFAULTS = [
    (re.compile(r"row-level triggers by default", re.I), "CDC trigger default is statement-level, not row-level"),
    (re.compile(r"row-level AFTER triggers \(the default\)", re.I), "CDC trigger default is statement-level"),
    (re.compile(r"default CDC mode \(row-level", re.I), "CDC trigger default is statement-level"),
    (re.compile(r"parallel_refresh_mode\s*=\s*['\"]off['\"]\s*\)", re.I), "parallel_refresh_mode default is 'on', not 'off'"),
    (re.compile(r"default\s+off.*parallel_refresh_mode", re.I), "parallel_refresh_mode default is 'on'"),
    (re.compile(r"single scheduler worker", re.I), "Scheduler is launcher + per-database schedulers"),
    (re.compile(r"single background worker.*refresh", re.I), "Scheduler is launcher + per-database schedulers"),
]

# Lines/patterns that indicate negation or historical context — exempt from checks
NEGATION_PATTERNS = [
    re.compile(r"does not expose", re.I),
    re.compile(r"does not implement", re.I),
    re.compile(r"not part of.*current.*pg_trickle", re.I),
    re.compile(r"not.*current.*API", re.I),
    re.compile(r"moved to.*pg_tide", re.I),
    re.compile(r"extracted to.*pg_tide", re.I),
    re.compile(r"no longer", re.I),
    re.compile(r"historical", re.I),
    re.compile(r"pre-v0\.", re.I),
    re.compile(r"v0\.\d+\.0.*removed", re.I),
    re.compile(r"old.*function", re.I),
    re.compile(r"removed.*function", re.I),
    re.compile(r"were.*removed", re.I),
    re.compile(r"(pg_tide|historical|removed|pre-v0|legacy|troubleshoot)", re.I),
]

# Files to skip entirely (historical changelog, upgrade notes, test fixtures)
SKIP_FILES = {
    "UPGRADING.md",
    "changelog.md",
    "WHATS_NEW.md",
    "GUC_CATALOG.md",
    "SQL_API_CATALOG.md",
    "ERRORS.md",
}

# Files where historical API mentions are expected (skip removed-API check)
HISTORICAL_FILES = {
    "UPGRADING.md",
    "changelog.md",
    "WHATS_NEW.md",
    "OUTBOX.md",  # explicitly states old API is gone
    "INBOX.md",  # explicitly states old API is gone
    "PATTERNS.md",  # pattern notes explain old vs new
    "SQL_REFERENCE.md",  # pg_tide section explicitly lists removed names
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_allowlist() -> tuple[set[str], set[str]]:
    """Load the YAML allowlist of exempted GUC and SQL function patterns."""
    guc_exemptions: set[str] = set()
    sql_exemptions: set[str] = set()

    if not ALLOWLIST_PATH.exists():
        return guc_exemptions, sql_exemptions

    text = ALLOWLIST_PATH.read_text(encoding="utf-8")
    current_section = None

    for line in text.splitlines():
        line = line.rstrip()
        if line.startswith("guc_exemptions:"):
            current_section = "guc"
        elif line.startswith("sql_function_exemptions:"):
            current_section = "sql"
        elif line.strip().startswith("- pattern:"):
            val = line.split("- pattern:", 1)[1].strip()
            if current_section == "guc":
                guc_exemptions.add(val)
            elif current_section == "sql":
                sql_exemptions.add(val)

    return guc_exemptions, sql_exemptions


def load_guc_names(path: Path) -> set[str]:
    """Extract `pg_trickle.<name>` GUC names from the generated catalog."""
    if not path.exists():
        return set()
    names = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.search(r"`(pg_trickle\.[^`]+)`", line)
        if m:
            names.add(m.group(1))
    return names


def load_sql_function_names(path: Path) -> set[str]:
    """Extract `pgtrickle.<name>` function names from the generated catalog."""
    if not path.exists():
        return set()
    names = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.search(r"`(pgtrickle\.[^`(]+)", line)
        if m:
            names.add(m.group(1).rstrip("(").strip())
    return names


def is_negation_context(line: str) -> bool:
    """Return True if the line uses a negation / historical framing."""
    return any(p.search(line) for p in NEGATION_PATTERNS)


def is_in_code_fence(lineno: int, fence_ranges: list) -> bool:
    """Return True if lineno is inside a fenced code block."""
    return any(start <= lineno <= end for start, end in fence_ranges)


def build_fence_ranges(lines: list[str]) -> list:
    """Return list of (start_lineno, end_lineno) for fenced code blocks."""
    ranges = []
    in_fence = False
    fence_start = 0
    for i, line in enumerate(lines, start=1):
        if line.strip().startswith("```"):
            if not in_fence:
                in_fence = True
                fence_start = i
            else:
                ranges.append((fence_start, i))
                in_fence = False
    return ranges


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_guc_references(path: Path, lines: list, fences: list, guc_names: set, guc_exemptions: set) -> list:
    """Check for pg_trickle.XXX GUC references not in the generated catalog."""
    issues = []
    # Pattern: backtick-quoted pg_trickle.xxx references (not inside code fences)
    pat = re.compile(r"`(pg_trickle\.[a-z_]+)`")
    for i, line in enumerate(lines, start=1):
        if is_in_code_fence(i, fences):
            continue
        for m in pat.finditer(line):
            name = m.group(1)
            # Skip prefixes that we know no longer exist (reported by removed-guc check)
            if any(name.startswith(p) for p in REMOVED_GUC_PREFIXES):
                continue
            if name in guc_exemptions:
                continue
            if name not in guc_names:
                issues.append((i, f"Unknown GUC reference: `{name}` — not in GUC_CATALOG.md"))
    return issues


def check_sql_function_references(
    path: Path, lines: list, fences: list, fn_names: set, sql_exemptions: set
) -> list:
    """Check for pgtrickle.xxx() SQL calls not in the generated catalog."""
    issues = []
    # Match pgtrickle.xxx( — inside prose or code
    pat = re.compile(r"pgtrickle\.([a-z_]+)\s*\(")
    # Internal helper prefixes to skip
    skip_prefixes = {"_"}
    for i, line in enumerate(lines, start=1):
        for m in pat.finditer(line):
            fn = f"pgtrickle.{m.group(1)}"
            if m.group(1).startswith(tuple(skip_prefixes)):
                continue
            if fn in sql_exemptions:
                continue
            if fn not in fn_names:
                issues.append((i, f"Unknown SQL function reference: `{fn}()` — not in SQL_API_CATALOG.md"))
    return issues


def check_removed_apis(path: Path, lines: list, fences: list) -> list:
    """Check for removed pg_trickle-owned APIs used as if they were active."""
    issues = []
    for i, line in enumerate(lines, start=1):
        if is_negation_context(line):
            continue
        for api in REMOVED_PGTRICKLE_APIS:
            if re.search(rf"\b{api}\b", line, re.I):
                # Check if it's wrapped in negation context (broader window)
                window = "\n".join(lines[max(0, i - 3) : min(len(lines), i + 2)])
                if not is_negation_context(window):
                    issues.append(
                        (i, f"Removed pg_trickle API mentioned as active: `{api}` — moved to pg_tide v0.46.0")
                    )
    return issues


def check_removed_gucs(path: Path, lines: list, fences: list) -> list:
    """Check for removed GUC prefixes outside negation context."""
    issues = []
    pat = re.compile(r"`(pg_trickle\.(outbox|inbox|consumer)_[^`]+)`")
    for i, line in enumerate(lines, start=1):
        for m in pat.finditer(line):
            name = m.group(1)
            window = "\n".join(lines[max(0, i - 2) : min(len(lines), i + 2)])
            if not is_negation_context(window):
                issues.append(
                    (i, f"Removed GUC referenced: `{name}` — outbox/inbox/consumer GUCs moved to pg_tide")
                )
    return issues


def check_stale_defaults(path: Path, lines: list, fences: list) -> list:
    """Check for stale default-wording patterns."""
    issues = []
    for i, line in enumerate(lines, start=1):
        if is_in_code_fence(i, fences):
            continue
        for pattern, note in STALE_DEFAULTS:
            if pattern.search(line):
                issues.append((i, f"Stale default claim: {note}"))
    return issues


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------


def scan_file(
    path: Path,
    guc_names: set,
    fn_names: set,
    skip_historical: bool,
    guc_exemptions: set,
    sql_exemptions: set,
) -> list:
    """Scan a single Markdown file and return a list of (lineno, message) tuples."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return []

    lines = text.splitlines()
    fences = build_fence_ranges(lines)
    issues = []

    issues += check_guc_references(path, lines, fences, guc_names, guc_exemptions)
    issues += check_sql_function_references(path, lines, fences, fn_names, sql_exemptions)
    if not skip_historical:
        issues += check_removed_apis(path, lines, fences)
        issues += check_removed_gucs(path, lines, fences)
    issues += check_stale_defaults(path, lines, fences)

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check pg_trickle docs for stale API references and GUC drift."
    )
    parser.add_argument(
        "--docs-dir",
        default=str(DOCS_DIR),
        help="Root docs directory to scan (default: docs/).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print each checked file.",
    )
    args = parser.parse_args()

    docs_dir = Path(args.docs_dir)

    # Load source-of-truth catalogs
    if not GUC_CATALOG_PATH.exists():
        print(
            f"ERROR: {GUC_CATALOG_PATH} not found. Run `python3 scripts/gen_catalogs.py` first.",
            file=sys.stderr,
        )
        return 1

    if not SQL_CATALOG_PATH.exists():
        print(
            f"ERROR: {SQL_CATALOG_PATH} not found. Run `python3 scripts/gen_catalogs.py` first.",
            file=sys.stderr,
        )
        return 1

    guc_names = load_guc_names(GUC_CATALOG_PATH)
    fn_names = load_sql_function_names(SQL_CATALOG_PATH)
    guc_exemptions, sql_exemptions = load_allowlist()

    print(
        f"Loaded {len(guc_names)} GUCs, {len(fn_names)} SQL functions, "
        f"{len(guc_exemptions)} GUC exemptions, {len(sql_exemptions)} SQL exemptions."
    )

    # Collect all Markdown files recursively
    md_files = sorted(docs_dir.rglob("*.md"))

    total_issues = 0
    for md_path in md_files:
        rel = md_path.relative_to(REPO_ROOT)
        fname = md_path.name

        if fname in SKIP_FILES:
            if args.verbose:
                print(f"  SKIP {rel}")
            continue

        skip_historical = fname in HISTORICAL_FILES

        if args.verbose:
            print(f"  CHECK {rel}")

        issues = scan_file(md_path, guc_names, fn_names, skip_historical, guc_exemptions, sql_exemptions)
        if issues:
            for lineno, msg in issues:
                print(f"{rel}:{lineno}: {msg}")
            total_issues += len(issues)

    if total_issues:
        print(f"\nFound {total_issues} issue(s). Fix before merging.", file=sys.stderr)
        return 1

    print(f"Checked {len(md_files)} files — no issues found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
