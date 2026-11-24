#!/usr/bin/env python3
"""Validate review deadlines and export the shared RustSec advisory ignore list.

Each advisory in deny.toml must have one preceding Review-By comment.
Run without arguments to check expiry, or with --audit-ignores to emit only
comma-separated IDs for rustsec/audit-check (after validating all deadlines).
"""
import argparse
import re
import sys
import tomllib
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DENY_TOML = REPO_ROOT / "deny.toml"
_REVIEW_BY_RE = re.compile(r"#\s*Review-By:\s*(\d{4}-\d{2}-\d{2})")
_ADVISORY_RE = re.compile(r'"(RUSTSEC-\d{4}-\d{4,})"\s*,?')


def review_dates(text: str) -> dict[str, date]:
    ignores = tomllib.loads(text)["advisories"]["ignore"]
    if not isinstance(ignores, list) or any(not isinstance(i, str) for i in ignores):
        raise ValueError("advisory ignores must be a list of RustSec IDs")
    if len(set(ignores)) != len(ignores):
        raise ValueError("duplicate advisory ignore")
    if not ignores:
        return {}

    result: dict[str, date] = {}
    in_advisories = False
    in_ignores = False
    pending: date | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if not in_ignores and line.startswith("["):
            in_advisories = line == "[advisories]"
            continue
        if not in_advisories:
            continue
        if not in_ignores:
            in_ignores = re.fullmatch(r"ignore\s*=\s*\[", line) is not None
            continue
        if line == "]":
            break
        if line.startswith("#"):
            if re.match(r"#\s*Review-By:", line):
                match = _REVIEW_BY_RE.fullmatch(line)
                if match is None or pending is not None:
                    raise ValueError("invalid or duplicate Review-By comment")
                pending = date.fromisoformat(match.group(1))
        elif line:
            match = _ADVISORY_RE.fullmatch(line)
            if match is None:
                raise ValueError(f"invalid advisory ignore entry: {line}")
            advisory = match.group(1)
            if pending is None:
                raise ValueError(f"{advisory} has no Review-By date")
            result[advisory] = pending
            pending = None
    if pending is not None or list(result) != ignores:
        raise ValueError("Review-By comments must match every advisory ignore in order")
    return result


def validated_ignores(text: str, today: date) -> dict[str, date]:
    dates = review_dates(text)
    expired = [f"{adv} ({rd})" for adv, rd in dates.items() if rd < today]
    if expired:
        raise ValueError("expired advisory ignores: " + ", ".join(expired))
    return dates


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-ignores", action="store_true")
    args = parser.parse_args(argv)
    try:
        dates = validated_ignores(DENY_TOML.read_text(encoding="utf-8"), date.today())
    except OSError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except (ValueError, KeyError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.audit_ignores:
        print(",".join(dates))
    else:
        print(f"OK: {len(dates)} advisory ignore(s) within review window:")
        for adv, rd in dates.items():
            print(f"  {adv}  (review by {rd})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
