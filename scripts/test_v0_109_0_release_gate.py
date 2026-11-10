import copy
import json
import unittest
from unittest.mock import patch

import run_release_pgvector_suite as pgvector_suite
import v0_109_0_release_gate as gate


class RetirementContractTests(unittest.TestCase):
    def test_published_vector_upgrade_checks_cannot_be_removed(self):
        script = (gate.prior.ROOT / "scripts/qualify_live_package_upgrade.sh").read_text()
        gate.check_pgvector_upgrade_script(script)
        with self.assertRaises(SystemExit):
            gate.check_pgvector_upgrade_script(script.replace("bool_and(pgt_id IS NULL)", "true"))

    def test_pgvector_runner_selects_declared_cases_and_version(self):
        contract = json.loads((gate.prior.ROOT / "tests/release/v0.109.0-qualification.json").read_text())
        cases = next(s for s in contract["required_suites"] if s["id"] == "pgvector-0.8.7")["required_cases"]
        with patch("sys.argv", ["runner", "0.8.7"]), patch.object(pgvector_suite.subprocess, "run") as run:
            run.return_value.returncode = 0
            self.assertEqual(pgvector_suite.main(), 0)
        command = run.call_args.args[0]
        for case in cases:
            binary, name = case.split("::", 1)
            self.assertIn(binary, command)
            self.assertIn(f"test({name})", command[-1])
        self.assertEqual(run.call_args.kwargs["env"]["PGVECTOR_EXPECTED_VERSION"], "0.8.7")

    def test_pgvector_contract_rejects_missing_cases_and_wrong_image(self):
        contract = json.loads((gate.prior.ROOT / "tests/release/v0.109.0-qualification.json").read_text())
        gate.check_pgvector_contract(contract)
        for field, value in [("required_cases", []), ("e2e_image", "pg_trickle_e2e:latest"), ("runtime", False)]:
            broken = copy.deepcopy(contract)
            next(s for s in broken["required_suites"] if s["id"] == "pgvector-0.8.1")[field] = value
            with self.assertRaises(SystemExit):
                gate.check_pgvector_contract(broken)
        broken = copy.deepcopy(contract)
        next(s for s in broken["required_suites"] if s["id"] == "pgvector-catalog-upgrade")["required_cases"].pop()
        with self.assertRaises(SystemExit):
            gate.check_pgvector_contract(broken)

        for version in ("0.8.1", "0.8.7"):
            for case in ("test_live_rag_converges_after_writes_during_maintenance", "test_live_rag_converges_after_writes_during_active_maintenance", "test_live_rag_recovers_after_maintenance_cancellation", "test_live_rag_recovers_after_database_crash_during_maintenance"):
                broken = copy.deepcopy(contract)
                cases = next(s for s in broken["required_suites"] if s["id"] == f"pgvector-{version}")["required_cases"]
                cases.remove(f"e2e_pgvector_revision_tests::{case}")
                with self.assertRaises(SystemExit):
                    gate.check_pgvector_contract(broken)

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
