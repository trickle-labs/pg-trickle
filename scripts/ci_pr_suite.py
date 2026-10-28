#!/usr/bin/env python3
"""Select the bounded installed-extension PR suite; fail on unmapped targets."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEDULER_ISOLATED = {
    "e2e_dag_autorefresh_tests", "e2e_mixed_mode_dag_tests",
    "e2e_mixed_scheduling_tests", "e2e_ownership_tests", "e2e_scheduler_tests",
    # This matrix checks WAL receipt and scheduler progress across routes.
    "e2e_dvm_strategy_tests",
    "e2e_v107_wal_transition_tests", "e2e_rls_wal_tests",
    "e2e_watermark_gating_tests", "e2e_bootstrap_gating_tests",
    # This failpoint witness requires deterministic scheduler progress.
    "e2e_dvm_failpoint_tests",
}
SERIAL_CASES = [
    ("e2e_rls_wal_tests", "test_rls_bypassrls_owner_differential_wal_matches_query"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_before_receipt_commit_preserves_committed_changes"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_receipt_commit_before_ack_converges_once"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_apply_before_ack_does_not_reapply"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_slot_advance_finishes_acknowledgement"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_receipt_commit_order_preserves_late_old_lsn_transaction"),
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
    for binary, test in SERIAL_CASES:
        source = (root / "tests" / f"{binary}.rs").read_text()
        if binary not in full or f"fn {test}(" not in source:
            raise ValueError(f"missing serial correctness case: {binary}::{test}")
    return full


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="validate and list targets without Docker")
    parser.add_argument("--no-fail-fast", action="store_true", help="retain all selected failure evidence")
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
    # The paired allocation serializes high-risk WAL/LSN cases separately, but
    # bootstrap and watermark scheduler binaries also need uncontended workers.
    isolated = [name for name in selected if name in SCHEDULER_ISOLATED]
    command = ["./scripts/run_e2e_tests.sh", "--profile", "ci", "--retries", "0"]
    if args.no_fail_fast:
        command.append("--no-fail-fast")
    for name in selected:
        if name != "e2e_refresh_atomicity_tests" and name not in SCHEDULER_ISOLATED:
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
    serial_cases = [
        (binary, test) for binary, test in SERIAL_CASES
        if binary in selected and binary not in SCHEDULER_ISOLATED
    ]
    if not serial_cases:
        return 0
    serial = ["./scripts/run_e2e_tests.sh", "--profile", "ci", "--retries", "0",
              "--no-fail-fast", "--test-threads", "1"]
    for binary in dict.fromkeys(binary for binary, _ in serial_cases):
        serial.extend(["--test", binary])
    serial.extend(["-E", " | ".join(f"test(={test})" for _, test in serial_cases)])
    return subprocess.run(serial, cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
