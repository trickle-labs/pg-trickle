#!/usr/bin/env python3
"""Select the bounded installed-extension PR suite; fail on unmapped targets."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERIAL_CASES = [
    ("e2e_rls_wal_tests", "test_rls_bypassrls_owner_differential_wal_matches_query"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_before_receipt_commit_preserves_committed_changes"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_receipt_commit_before_ack_converges_once"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_apply_before_ack_does_not_reapply"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_slot_advance_finishes_acknowledgement"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_trigger_fallback_received_receipt_requires_rebaseline"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_trigger_fallback_undecoded_commit_requires_rebaseline"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_semantic_controls_are_detected"),
    ("e2e_v043_tests", "test_t_a44_10_insert_correctness"),
    ("e2e_v107_wal_transition_tests", "test_v107_auto_wal_transition_captures_changes"),
    ("e2e_lsn_contract_tests", "test_lsn_manual_refresh_fixed_snapshots_preserve_frontier"),
    ("e2e_lsn_contract_tests", "test_lsn_scheduler_cdc_holdback_recovers_after_long_writer"),
    ("e2e_property_circular_tests", "test_prop_monotone_cycle_small_graph_converges"),
    ("e2e_dag_autorefresh_tests", "test_autorefresh_no_spurious_3_layer"),
    ("e2e_refresh_tests", "test_stddev_samp_differential_mode"),
    ("e2e_v0878_security_tests", "test_v0878_revoked_source_preserves_state_and_stages"),
]


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
    for binary, test in SERIAL_CASES:
        source = (root / "tests" / f"{binary}.rs").read_text()
        if binary not in full or f"fn {test}(" not in source:
            raise ValueError(f"missing serial correctness case: {binary}::{test}")
    return full


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="validate and list targets without Docker")
    parser.add_argument("--no-fail-fast", action="store_true", help="retain all selected failure evidence")
    args = parser.parse_args()
    selected = targets()
    if args.list:
        print("\n".join(selected))
        return 0
    command = ["./scripts/run_e2e_tests.sh", "--profile", "ci", "--retries", "0"]
    if args.no_fail_fast:
        command.append("--no-fail-fast")
    for name in selected:
        if name != "e2e_refresh_atomicity_tests":
            command.extend(["--test", name])
    cases = json.loads((ROOT / ".github/ci-suites.json").read_text())["scheduled_cases"]
    excluded = []
    for case in cases:
        binary, test = case.split("::")
        excluded.append(f"not (binary(={binary}) & test(={test}))")
    for binary, test in SERIAL_CASES:
        excluded.append(f"not (binary(={binary}) & test(={test}))")
    if excluded:
        command.extend(["-E", " & ".join(excluded)])
    status = subprocess.run(command, cwd=ROOT).returncode
    if status:
        return status
    # Keep shared-worker and barrier cases isolated from the parallel suite load.
    status = subprocess.run([
        "./scripts/run_e2e_tests.sh", "--profile", "ci",
        "--test", "e2e_refresh_atomicity_tests", "--test-threads", "1", "--retries", "0",
    ], cwd=ROOT).returncode
    if status:
        return status
    serial = ["./scripts/run_e2e_tests.sh", "--profile", "ci", "--retries", "0",
              "--no-fail-fast", "--test-threads", "1"]
    for binary in dict.fromkeys(binary for binary, _ in SERIAL_CASES):
        serial.extend(["--test", binary])
    serial.extend(["-E", " | ".join(f"test(={test})" for _, test in SERIAL_CASES)])
    return subprocess.run(serial, cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
