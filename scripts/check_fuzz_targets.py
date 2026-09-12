#!/usr/bin/env python3
"""Validate that smoke and nightly consume the Cargo fuzz inventory."""
from pathlib import Path
import re
import sys
from fuzz_targets import targets as cargo_targets

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = [
    ROOT / ".github" / "workflows" / "fuzz-smoke.yml",
    ROOT / ".github" / "workflows" / "fuzz-nightly.yml",
]


def main() -> int:
    targets = cargo_targets()
    if not targets:
        print("ERROR: no fuzz targets in fuzz/Cargo.toml", file=sys.stderr)
        return 2
    for workflow in WORKFLOWS:
        if not workflow.exists():
            print(f"ERROR: {workflow} not found", file=sys.stderr)
            return 2
        text = workflow.read_text(encoding="utf-8")
        if "scripts/fuzz_targets.py" not in text:
            print(f"ERROR: {workflow.name} does not consume scripts/fuzz_targets.py", file=sys.stderr)
            return 1
        if re.search(r"TARGETS=\([^)]*(?:_fuzz)", text):
            print(f"ERROR: {workflow.name} contains a hard-coded fuzz inventory", file=sys.stderr)
            return 1
    print(f"OK: {len(targets)} targets are inventory-driven in smoke and nightly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
