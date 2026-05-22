#!/usr/bin/env python3
"""check_deny_expiry.py — DEP-001: Fail if any RUSTSEC ignore is past its review date.

Parses deny.toml and finds all `# Review-By: YYYY-MM-DD` comment lines.
Exits with code 1 if any review date is in the past, prompting a human to
re-evaluate whether the advisory ignore is still appropriate.

Usage:
    python3 scripts/check_deny_expiry.py
"""
import re
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DENY_TOML = REPO_ROOT / "deny.toml"

_REVIEW_BY_RE = re.compile(r"#\s*Review-By:\s*(\d{4}-\d{2}-\d{2})")
# Also capture the advisory ID that precedes the review line
_ADVISORY_RE = re.compile(r'"(RUSTSEC-\d{4}-\d{4})"')


def main() -> int:
    if not DENY_TOML.exists():
        print(f"ERROR: {DENY_TOML} not found", file=sys.stderr)
        return 2

    today = date.today()
    text = DENY_TOML.read_text(encoding="utf-8")
    lines = text.splitlines()

    expired: list[tuple[str, str, date]] = []
    ok: list[tuple[str, str, date]] = []

    # Walk forward from each Review-By line to find the associated advisory ID
    for i, line in enumerate(lines):
        m = _REVIEW_BY_RE.search(line)
        if not m:
            continue
        review_date = date.fromisoformat(m.group(1))
        # Search forward (up to 10 lines) for the RUSTSEC ID, then try backward
        advisory_id = "UNKNOWN"
        for j in range(i + 1, min(i + 10, len(lines))):
            am = _ADVISORY_RE.search(lines[j])
            if am:
                advisory_id = am.group(1)
                break
        if advisory_id == "UNKNOWN":
            for j in range(i - 1, max(i - 10, -1), -1):
                am = _ADVISORY_RE.search(lines[j])
                if am:
                    advisory_id = am.group(1)
                    break

        if review_date < today:
            expired.append((advisory_id, line.strip(), review_date))
        else:
            ok.append((advisory_id, line.strip(), review_date))

    if ok:
        print(f"OK: {len(ok)} advisory ignore(s) within review window:")
        for adv, _, rd in ok:
            print(f"  {adv}  (review by {rd})")

    if expired:
        print(
            f"\nERROR: {len(expired)} advisory ignore(s) past their review date:",
            file=sys.stderr,
        )
        for adv, line_txt, rd in expired:
            print(f"  {adv}  — review date was {rd}", file=sys.stderr)
        print(
            "\nAction required: review each expired ignore in deny.toml and either:\n"
            "  (a) update the Review-By date if the ignore is still appropriate, or\n"
            "  (b) remove the ignore if the upstream dep has been updated.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
