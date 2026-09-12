#!/usr/bin/env python3
"""Emit the cargo-fuzz target inventory in deterministic order."""
from pathlib import Path
import sys
import tomllib


ROOT = Path(__file__).resolve().parent.parent
CARGO = ROOT / "fuzz" / "Cargo.toml"


def targets() -> list[str]:
    bins = tomllib.loads(CARGO.read_text(encoding="utf-8")).get("bin", [])
    return sorted(item["name"] for item in bins if "name" in item)


if __name__ == "__main__":
    if not CARGO.exists():
        print(f"ERROR: {CARGO} not found", file=sys.stderr)
        raise SystemExit(2)
    names = targets()
    if not names:
        print("ERROR: no [[bin]] targets found", file=sys.stderr)
        raise SystemExit(2)
    print("\n".join(names))
