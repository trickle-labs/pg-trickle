#!/usr/bin/env python3
"""Emit the cargo-fuzz target inventory in deterministic order."""
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent.parent
CARGO = ROOT / "fuzz" / "Cargo.toml"


def targets() -> list[str]:
    text = CARGO.read_text(encoding="utf-8")
    bins = re.findall(r"\[\[bin\]\](.*?)(?=\[\[bin\]\]|\[dependencies\])", text, re.S)
    return sorted(
        name
        for section in bins
        for name in re.findall(r"^\s*name\s*=\s*\"([^\"]+)\"\s*$", section, re.M)
    )


if __name__ == "__main__":
    if not CARGO.exists():
        print(f"ERROR: {CARGO} not found", file=sys.stderr)
        raise SystemExit(2)
    names = targets()
    if not names:
        print("ERROR: no [[bin]] targets found", file=sys.stderr)
        raise SystemExit(2)
    print("\n".join(names))
