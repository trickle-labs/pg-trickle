#!/usr/bin/env python3
"""Retain the prior release gates and require local WAL retirement qualification."""

import json

import v0_108_3_release_gate as prior


CASES = {
    "e2e_wal_receipt_recovery_tests::" + name
    for name in (
        "test_last_wal_consumer_drop_rollback_preserves_slot_and_frontier",
        "test_deferred_consumer_registration_rollback_preserves_wal_slot",
        "test_slot_retirement_commits_before_drop_and_recovers_after_drop_crash",
        "test_slot_retirement_keeps_active_stale_and_mismatched_slots",
        "test_dropped_wal_source_retains_retirement_until_last_consumer_commits",
        "test_slot_adoption_rejects_removed_consumer_and_recovers_orphan_after_crash",
    )
}


def check_retirement_contract(contract: dict) -> None:
    release_gate = next(item for item in contract["required_suites"] if item["id"] == "release-gate")
    prior.shared.require(
        release_gate["command_argv"] == ["python3", "scripts/v0_108_4_release_gate.py"],
        "release qualification must invoke the current retirement gate",
    )
    suite = next(item for item in contract["required_suites"] if item["id"] == "wal-retirement")
    prior.shared.require(
        set(suite["required_cases"]) == CASES and len(suite["required_cases"]) == len(CASES)
        and suite["command_argv"] == [
            "bash", "scripts/run_light_e2e_tests.sh", "--no-capture", "--test",
            "e2e_wal_receipt_recovery_tests",
        ]
        and suite["e2e_image"] == "pg_trickle_release_candidate:local",
        "retirement qualification must execute all named cases against the release candidate",
    )
    upgrade = next(item for item in contract["required_suites"] if item["id"] == "wal-retirement-upgrade")
    prior.shared.require(
        upgrade["command_argv"] == ["bash", "scripts/qualify_live_package_upgrade.sh", "0.108.3", "0.108.4"],
        "retirement upgrade must start from the actual published predecessor package",
    )


def main() -> None:
    prior.VERSION = "0.108.4"
    prior.PREVIOUS_VERSION = "0.108.3"
    prior.SOURCE_VERSIONS = ["0.108.3", "0.106.1"]
    prior.GATE_SCRIPT = "v0_108_4_release_gate.py"
    prior.REQUIRED_CASES = prior.REQUIRED_CASES | CASES
    prior.REQUIRED_SHARDS = prior.REQUIRED_SHARDS | {"wal-retirement"}
    prior.EXPECTED_SUITES = prior.EXPECTED_SUITES | {"wal-retirement", "wal-retirement-upgrade"}
    contract = json.loads((prior.ROOT / "tests/release/v0.108.4-qualification.json").read_text())
    check_retirement_contract(contract)
    prior.main()


if __name__ == "__main__":
    main()
