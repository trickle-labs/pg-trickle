"""Reject changes to the saved workload and SQL acceptance fixtures."""

import hashlib
import json
import re
from pathlib import Path

root = Path(__file__).resolve().parent
manifest = json.loads((root / "frozen-harness.json").read_text())
for name, expected in manifest["qualified_files"].items():
    actual = hashlib.sha256((root / "qualification" / name).read_bytes()).hexdigest()
    assert actual == expected, f"frozen S1 workload changed: {name}"
    if name.endswith(".sql"):
        fixture = root / "qualification" / name
        for included in re.findall(r"^\\ir\s+(\S+)", fixture.read_text(), re.MULTILINE):
            assert (fixture.parent / included).is_file(), f"missing S1 fixture: {included} in {name}"
print("frozen S1 workload and SQL fixture hashes: PASS")
