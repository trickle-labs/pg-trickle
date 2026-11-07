import copy
import json
import unittest

import v0_109_0_release_gate as gate


class RetirementContractTests(unittest.TestCase):
    def test_metadata_tracks_package_and_never_precedes_gate(self):
        check = gate.prior.shared.check_metadata_version
        check("0.110.0", "0.110.0", "0.109.0")
        check("0.109.0", "0.109.0", "0.109.0")
        for meta, package, version in [
            ("0.109.0", "0.110.0", "0.109.0"),
            ("0.108.3", "0.108.3", "0.109.0"),
        ]:
            with self.assertRaises(SystemExit):
                check(meta, package, version)

    def test_requires_pause_cases_in_recovery_suite_and_shard(self):
        contract = json.loads((gate.prior.ROOT / "tests/release/v0.109.0-qualification.json").read_text())
        self.assertTrue(gate.PAUSE_CASES <= set(contract["required_cases"]))
        for section, identity in [("required_shards", "failure-recovery"), ("required_suites", "recovery")]:
            selected = next(item for item in contract[section] if item["id"] == identity)
            self.assertTrue(gate.PAUSE_CASES <= set(selected["required_cases"]))

    def test_requires_runtime_cases_and_published_predecessor(self):
        contract = json.loads((gate.prior.ROOT / "tests/release/v0.109.0-qualification.json").read_text())
        gate.check_retirement_contract(contract)
        for suite_id, field, value in [
            ("release-gate", "command_argv", ["python3", "scripts/v0_108_3_release_gate.py"]),
            ("wal-retirement", "required_cases", sorted(gate.CASES)[:-1]),
            ("wal-retirement", "command_argv", ["true"]),
            ("wal-retirement-upgrade", "command_argv", ["bash", "scripts/qualify_live_package_upgrade.sh", "0.109.0", "0.109.0"]),
        ]:
            broken = copy.deepcopy(contract)
            next(s for s in broken["required_suites"] if s["id"] == suite_id)[field] = value
            with self.assertRaises(SystemExit):
                gate.check_retirement_contract(broken)
