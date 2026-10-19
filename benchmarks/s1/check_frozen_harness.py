"""Reject changes to the saved workload and SQL acceptance fixtures."""

import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
manifest = json.loads((root / "frozen-harness.json").read_text())
for name, expected in manifest["qualified_files"].items():
    actual = hashlib.sha256((root / "qualification" / name).read_bytes()).hexdigest()
    assert actual == expected, f"frozen S1 workload changed: {name}"
print("frozen S1 workload and SQL fixture hashes: PASS")
