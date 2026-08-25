#!/usr/bin/env python3
"""DX-2: SQL_REFERENCE.md Completeness Reconciliation.

Scans every ``#[pg_extern]`` annotation in ``src/`` and compares the
discovered function names against the headings in ``docs/SQL_REFERENCE.md``.
Exits non-zero (and prints a diff) when the reference is out of sync.

Usage
-----
    python3 scripts/gen_sql_reference.py             # check mode (default)
    python3 scripts/gen_sql_reference.py --list      # just print discovered names
    python3 scripts/gen_sql_reference.py --help

Internal functions
------------------
Functions whose SQL name starts with an underscore (``_on_ddl_end``,
``_on_sql_drop``, …) are considered *internal* and are excluded from the
completeness check.  To suppress a function entirely from SQL output,
annotate it with ``#[pg_extern(sql = "")]`` in Rust source.

Exit codes
----------
0 — reference is complete (or ``--list`` mode).
1 — one or more public ``#[pg_extern]`` functions are missing from the docs.
2 — usage error.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths (relative to repo root, resolved from this script's location)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
SQL_REF = REPO_ROOT / "docs" / "SQL_REFERENCE.md"

# Functions that are intentionally internal and excluded from the reference.
# Add entries here when a function is annotated #[pg_extern(sql = "")] in
# source but the regex below can't detect the suppression reliably.
KNOWN_INTERNAL: frozenset[str] = frozenset(
    {
        # DDL event hooks registered by CREATE EVENT TRIGGER, not callable by users.
        "_on_ddl_end",
        "_on_sql_drop",
        # VP (Validity Period) lifecycle hook, called only from CDC triggers.
        "handle_vp_promoted",
        # IVM delta application — called from AFTER trigger SQL bodies, not by users.
        "pgt_ivm_apply_delta",
        "pgt_ivm_apply_delta_enr",
        "pgt_ivm_handle_truncate",
    }
)

# Public functions that exist in source but are not yet documented in
# SQL_REFERENCE.md.  This "grace list" captures the state at v0.61.0 so CI
# fails only when a *new* undocumented function is added, not for pre-existing
# gaps.  Remove entries from here as you add documentation for them.
GRACE_UNDOCUMENTED: frozenset[str] = frozenset(
    {
        "attach_outbox",
        "bulk_alter_stream_tables",
        "bulk_drop_stream_tables",
        "cdc_pause_status",
        "clear_caches",
        "cluster_worker_summary",
        "convert_buffers_to_unlogged",
        "create_refresh_group",
        "dedup_stats",
        "detach_outbox",
        "diagnose_errors",
        "drain",
        "drop_refresh_group",
        "drop_snapshot",
        "drop_stream_table_publication",
        "embedding_stream_table",
        "exec_stream_ddl",
        "explain_dag",
        "explain_delta",
        "explain_diff_sql",
        "explain_query_rewrite",
        "explain_stream_table",
        "export_definition",
        "is_drained",
        "list_auxiliary_columns",
        "list_distance_subscriptions",
        "list_snapshots",
        "list_subscriptions",
        "metrics_summary",
        "migrate",
        "parse_duration_seconds",
        "pgt_scc_status",
        # Test-only SECURITY DEFINER probe, exported only with the pg_test feature.
        "pgt_test_capture_definer_path",
        "pgtrickle_refresh_stats",
        "preflight",
        "recommend_refresh_mode",
        "recommend_schedule",
        "refresh_efficiency",
        "refresh_groups",
        "reliability_counters",
        "restore_from_snapshot",
        "restore_stream_tables",
        "schedule_recommendations",
        "scheduler_overhead",
        "self_monitoring_status",
        "set_stream_table_sla",
        "setup_self_monitoring",
        "shared_buffer_stats",
        "sla_summary",
        "snapshot_stream_table",
        "source_stable_name",
        "st_auto_threshold",
        "stream_table_lineage",
        "subscribe",
        "subscribe_distance",
        "teardown_self_monitoring",
        "unsubscribe",
        "unsubscribe_distance",
        "validate_query",
        "vector_status",
        "version",
        "version_check",
        "view_evolution_status",
        "wal_source_status",
        "worker_allocation_status",
        "write_and_refresh",
    }
)

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Matches  #[pg_extern(schema = "pgtrickle", name = "foo", ...)]
_RE_NAMED = re.compile(
    r"""#\[pg_extern\s*\([^\)]*name\s*=\s*"([^"]+)"[^\)]*\)""",
    re.DOTALL,
)

# Matches  #[pg_extern(sql = "")]  — function is fully suppressed
_RE_SQL_EMPTY = re.compile(
    r"""#\[pg_extern\s*\([^\)]*sql\s*=\s*""[^\)]*\)""",
    re.DOTALL,
)

# Matches the Rust function name on the line immediately following a bare
#   #[pg_extern(...)]   (no explicit name = "...")
# e.g.:  pub fn create_stream_table(
#   or:  fn alter_stream_table(
#   or:  pub unsafe fn _internal_helper(
_RE_BARE_ATTR = re.compile(
    r"""#\[pg_extern\s*\([^\)]*\)\]""",
    re.DOTALL,
)

_RE_FN_NAME = re.compile(r"(?:pub\s+)?(?:unsafe\s+)?fn\s+(\w+)\s*[<\(]")

# Matches  ### pgtrickle.some_function_name  headings in SQL_REFERENCE.md
_RE_DOC_HEADING = re.compile(
    r"^#{1,4}\s+pgtrickle\.([a-z_][a-z0-9_]*)",
    re.MULTILINE,
)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_pg_extern_names(src_dir: Path) -> set[str]:
    """Return the set of public SQL function names exported via ``#[pg_extern]``."""
    names: set[str] = set()

    for rs_file in sorted(src_dir.rglob("*.rs")):
        text = rs_file.read_text(encoding="utf-8", errors="replace")

        # Walk line by line to handle proximity between attribute and fn declaration.
        lines = text.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i]

            # ── Named form: name = "foo" inside the attribute ──────────────
            m = _RE_NAMED.search(line)
            if m:
                # Make sure it isn't sql = "" suppressed on the same line.
                if not _RE_SQL_EMPTY.search(line):
                    names.add(m.group(1))
                i += 1
                continue

            # ── Bare form: #[pg_extern(schema = "pgtrickle")] ─────────────
            if _RE_BARE_ATTR.search(line) and "sql" not in line:
                # Look ahead up to 10 lines for the fn declaration, skipping
                # over any intervening attribute lines (#[allow(...)], etc.).
                for j in range(i + 1, min(i + 11, len(lines))):
                    next_line = lines[j].strip()
                    # Skip empty lines and other attribute lines.
                    if not next_line or next_line.startswith("#["):
                        continue
                    fn_m = _RE_FN_NAME.search(lines[j])
                    if fn_m:
                        names.add(fn_m.group(1))
                        break

            i += 1

    return names


def discover_doc_names(sql_ref: Path) -> set[str]:
    """Return the set of function names documented in SQL_REFERENCE.md."""
    text = sql_ref.read_text(encoding="utf-8")
    # Normalise Markdown backslash-escaped underscores before matching.
    text = text.replace(r"\_", "_")
    return {m.group(1) for m in _RE_DOC_HEADING.finditer(text)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print discovered public function names and exit 0.",
    )
    args = parser.parse_args(argv)

    # Resolve paths.
    if not SRC_DIR.is_dir():
        print(f"ERROR: src/ directory not found at {SRC_DIR}", file=sys.stderr)
        return 2
    if not SQL_REF.is_file():
        print(f"ERROR: {SQL_REF} not found", file=sys.stderr)
        return 2

    raw_names = discover_pg_extern_names(SRC_DIR)

    # Filter: drop known-internal names and current grace-listed names.
    public_names = {
        n
        for n in raw_names
        if n not in KNOWN_INTERNAL
        and n not in GRACE_UNDOCUMENTED
        and not n.startswith("_")
    }

    if args.list:
        for name in sorted(public_names):
            print(f"  pgtrickle.{name}")
        return 0

    doc_names = discover_doc_names(SQL_REF)

    missing = sorted(public_names - doc_names)
    extra = sorted(doc_names - public_names)

    ok = True

    if missing:
        ok = False
        print(
            "ERROR: The following public #[pg_extern] functions are NOT documented"
            f" in {SQL_REF.relative_to(REPO_ROOT)}:",
            file=sys.stderr,
        )
        for name in missing:
            print(f"  + pgtrickle.{name}", file=sys.stderr)
        print(
            "\nAdd a '### pgtrickle.<name>' heading to docs/SQL_REFERENCE.md"
            " or mark the function internal with #[pg_extern(sql = \"\")].",
            file=sys.stderr,
        )

    if extra:
        # Extra headings in the doc are warnings, not errors — a function may
        # have been removed or renamed.
        print(
            f"WARNING: {len(extra)} heading(s) in SQL_REFERENCE.md have no"
            " matching #[pg_extern] in source (function removed or renamed?):",
            file=sys.stdout,
        )
        for name in extra:
            print(f"  - pgtrickle.{name}")

    if ok:
        print(
            f"OK: all {len(public_names)} public #[pg_extern] functions are documented."
        )
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
