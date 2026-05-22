#!/usr/bin/env python3
"""check_fuzz_targets.py — CI-001: Validate fuzz-smoke.yml covers every target.

Reads all *.rs files under fuzz/fuzz_targets/ and checks that each one is
mentioned in .github/workflows/fuzz-smoke.yml.  Fails with a non-zero exit
code if any target file is absent from the workflow, preventing silent drift
when a new fuzz target is added without updating the workflow.

Usage:
    python3 scripts/check_fuzz_targets.py
"""
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FUZZ_DIR = REPO_ROOT / "fuzz" / "fuzz_targets"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "fuzz-smoke.yml"


def main() -> int:
    if not FUZZ_DIR.is_dir():
        print(f"ERROR: {FUZZ_DIR} not found", file=sys.stderr)
        return 2
    if not WORKFLOW.exists():
        print(f"ERROR: {WORKFLOW} not found", file=sys.stderr)
        return 2

    # Collect fuzz target names from the filesystem
    fs_targets = sorted(p.stem for p in FUZZ_DIR.glob("*.rs"))
    if not fs_targets:
        print("ERROR: no *.rs files found under fuzz/fuzz_targets/", file=sys.stderr)
        return 2

    # Read the workflow file and collect every word that looks like a target name
    # (appears as a bare word adjacent to known targets in the TARGETS arrays)
    workflow_text = WORKFLOW.read_text(encoding="utf-8")

    missing = []
    for target in fs_targets:
        # The target name must appear as a word boundary in the workflow YAML
        if not re.search(r'\b' + re.escape(target) + r'\b', workflow_text):
            missing.append(target)

    if missing:
        print("ERROR: The following fuzz targets are NOT listed in fuzz-smoke.yml:", file=sys.stderr)
        for t in missing:
            print(f"  {t}", file=sys.stderr)
        print(
            "\nAdd them to the TARGETS array in .github/workflows/fuzz-smoke.yml",
            file=sys.stderr,
        )
        return 1

    print(f"OK: all {len(fs_targets)} fuzz targets are covered in fuzz-smoke.yml")
    for t in fs_targets:
        print(f"  {t}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
