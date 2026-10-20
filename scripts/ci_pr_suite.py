#!/usr/bin/env python3
"""Select the bounded installed-extension PR suite; fail on unmapped targets."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def targets(root: Path = ROOT) -> list[str]:
    policy = json.loads((root / ".github/ci-suites.json").read_text())
    runner = (root / "scripts/run_light_e2e_tests.sh").read_text()
    light = set(re.findall(r"^    (e2e_\w+)$", runner, re.MULTILINE))
    inventory = {path.stem for path in (root / "tests").glob("e2e_*.rs")}
    full = policy["pr_full"]
    deferred = set(policy["scheduled_only"])
    if len(full) != len(set(full)) or deferred & (set(full) | light):
        raise ValueError("duplicate or conflicting CI suite allocation")
    allocated = light | set(full) | deferred
    if allocated != inventory:
        raise ValueError(
            f"unmapped targets: {sorted(inventory - allocated)}; "
            f"missing targets: {sorted(allocated - inventory)}"
        )
    # Light builds omit these cases entirely, even when the binary is allowlisted.
    for name in light:
        source = (root / "tests" / f"{name}.rs").read_text()
        if '#[cfg(not(feature = "light-e2e"))]' in source and name not in full:
            raise ValueError(f"full-harness cases omitted from PR: {name}")
    for case in policy["scheduled_cases"]:
        binary, test = case.split("::")
        source = (root / "tests" / f"{binary}.rs").read_text()
        if binary not in full or f"fn {test}(" not in source:
            raise ValueError(f"missing scheduled combination: {case}")
    return full


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="validate and list targets without Docker")
    args = parser.parse_args()
    selected = targets()
    if args.list:
        print("\n".join(selected))
        return 0
    command = ["./scripts/run_e2e_tests.sh", "--profile", "ci", "--retries", "0"]
    for name in selected:
        if name != "e2e_refresh_atomicity_tests":
            command.extend(["--test", name])
    cases = json.loads((ROOT / ".github/ci-suites.json").read_text())["scheduled_cases"]
    excluded = [
        "not (binary(=e2e_wal_receipt_recovery_tests) & "
        "test(=test_wal_trigger_fallback_received_receipt_requires_rebaseline))"
    ]
    for case in cases:
        binary, test = case.split("::")
        excluded.append(f"not (binary(={binary}) & test(={test}))")
    if excluded:
        command.extend(["-E", " & ".join(excluded)])
    status = subprocess.run(command, cwd=ROOT).returncode
    if status:
        return status
    # Keep witnessed barriers isolated from the parallel suite's container load.
    status = subprocess.run([
        "./scripts/run_e2e_tests.sh", "--profile", "ci",
        "--test", "e2e_refresh_atomicity_tests", "--test-threads", "1", "--retries", "0",
    ], cwd=ROOT).returncode
    if status:
        return status
    return subprocess.run([
        "./scripts/run_e2e_tests.sh", "--profile", "ci",
        "--test", "e2e_wal_receipt_recovery_tests",
        "-E", "test(test_wal_trigger_fallback_received_receipt_requires_rebaseline)",
        "--test-threads", "1", "--retries", "0",
    ], cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
