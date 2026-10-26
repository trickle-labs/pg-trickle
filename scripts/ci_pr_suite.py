#!/usr/bin/env python3
"""Select the bounded installed-extension PR suite; fail on unmapped targets."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Keep WAL receipt phase-barrier witnesses serial without removing them from PR coverage.
WAL_SERIAL_CASES = (
    ("e2e_wal_receipt_recovery_tests", "test_wal_trigger_fallback_received_receipt_requires_rebaseline"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_trigger_fallback_undecoded_commit_requires_rebaseline"),
)


def targets(root: Path = ROOT) -> list[str]:
    policy = json.loads((root / ".github/ci-suites.json").read_text())
    runner = (root / "scripts/run_light_e2e_tests.sh").read_text()
    light = set(re.findall(r"^    (e2e_\w+)$", runner, re.MULTILINE))
    inventory = {path.stem for path in (root / "tests").glob("e2e_*.rs")}
    full = policy["pr_full"]
    pure = policy["pr_pure"]
    shard_2 = policy["pr_full_shard_2"]
    if (len(shard_2) != len(set(shard_2)) or not set(shard_2) <= set(full)
            or (full and (not shard_2 or set(shard_2) == set(full)))
            or "e2e_refresh_atomicity_tests" in shard_2):
        raise ValueError("invalid full-suite shard allocation")
    deferred = set(policy["scheduled_only"])
    if (len(full) != len(set(full)) or len(pure) != len(set(pure))
            or set(pure) & (set(full) | light | deferred)
            or deferred & (set(full) | light)):
        raise ValueError("duplicate or conflicting CI suite allocation")
    allocated = light | set(full) | set(pure) | deferred
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
    for name in pure:
        source = (root / "tests" / f"{name}.rs").read_text()
        if "#[tokio::test]" in source or "E2eDb::" in source:
            raise ValueError(f"database test allocated to pure tier: {name}")
    for case in policy["scheduled_cases"]:
        binary, test = case.split("::")
        source = (root / "tests" / f"{binary}.rs").read_text()
        if binary not in full or f"fn {test}(" not in source:
            raise ValueError(f"missing scheduled combination: {case}")
    for case in policy.get("isolated_cases", {}):
        binary, test = case.split("::")
        source = (root / "tests" / f"{binary}.rs").read_text()
        if binary not in full or f"fn {test}(" not in source:
            raise ValueError(f"missing isolated combination: {case}")
    return full


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="validate and list targets without Docker")
    parser.add_argument("--pure", action="store_true", help="run database-free harness contracts")
    parser.add_argument("--shard", type=int, choices=(1, 2), help="run one duration-balanced full shard")
    args = parser.parse_args()
    selected = targets()
    if args.shard:
        shard_2 = json.loads((ROOT / ".github/ci-suites.json").read_text())["pr_full_shard_2"]
        selected = [name for name in selected if (name in shard_2) == (args.shard == 2)]
    if args.list:
        print("\n".join(selected))
        return 0
    if args.pure:
        command = ["cargo", "test", "--features", "pg18"]
        for name in json.loads((ROOT / ".github/ci-suites.json").read_text())["pr_pure"]:
            command.extend(["--test", name])
        return subprocess.run(command + ["--", "--test-threads=1"], cwd=ROOT).returncode
    # These scheduler-sensitive binaries use process-local shared containers.
    # Run them without parallel E2E contention so scheduler convergence
    # failures reflect behavior rather than runner resource pressure.
    scheduler_isolated = {
        "e2e_dag_autorefresh_tests", "e2e_mixed_mode_dag_tests",
        "e2e_mixed_scheduling_tests", "e2e_scheduler_tests",
        # The WAL transition waits for both decoder and scheduler progress.
        # Avoid CI refresh timeouts caused by parallel Docker contention.
        "e2e_v107_wal_transition_tests",
        # Scheduler and WAL progress assertions need an uncontended Docker host.
        "e2e_rls_wal_tests", "e2e_watermark_gating_tests",
    }
    isolated = [name for name in selected if name in scheduler_isolated]
    command = ["./scripts/run_e2e_tests.sh", "--profile", "ci", "--retries", "0"]
    for name in selected:
        if name != "e2e_refresh_atomicity_tests" and name not in scheduler_isolated:
            command.extend(["--test", name])
    cases = json.loads((ROOT / ".github/ci-suites.json").read_text())["scheduled_cases"]
    excluded = []
    for case in cases:
        binary, test = case.split("::")
        excluded.append(f"not (binary(={binary}) & test(={test}))")
    wal_serial_cases = [(binary, test) for binary, test in WAL_SERIAL_CASES if binary in selected]
    for binary, test in wal_serial_cases:
        excluded.append(f"not (binary(={binary}) & test(={test}))")
    if excluded:
        command.extend(["-E", " & ".join(excluded)])
    status = subprocess.run(command, cwd=ROOT).returncode
    if status:
        return status
    for name in isolated:
        status = subprocess.run([
            "./scripts/run_e2e_tests.sh", "--profile", "ci",
            "--test", name, "--test-threads", "1", "--retries", "0",
        ], cwd=ROOT).returncode
        if status:
            return status
    if "e2e_refresh_atomicity_tests" in selected:
        # Keep witnessed barriers isolated from parallel E2E container load.
        status = subprocess.run([
            "./scripts/run_e2e_tests.sh", "--profile", "ci",
            "--test", "e2e_refresh_atomicity_tests", "--test-threads", "1", "--retries", "0",
        ], cwd=ROOT).returncode
        if status:
            return status
    if not wal_serial_cases:
        return 0
    binary = wal_serial_cases[0][0]
    selector = " | ".join(f"test(={test})" for _, test in wal_serial_cases)
    return subprocess.run([
        "./scripts/run_e2e_tests.sh", "--profile", "ci",
        "--test", binary, "-E", selector,
        "--test-threads", "1", "--retries", "0",
    ], cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
