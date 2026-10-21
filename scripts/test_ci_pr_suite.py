"""Guard the PR allocation, runner invocation, and terminal merge verdict."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import check_ci_results
import ci_pr_suite
import run_ci_allocation


class AllocationTests(unittest.TestCase):
    def test_current_inventory_and_permanent_regressions_are_covered(self):
        full = ci_pr_suite.targets()
        for name in (
            "e2e_refresh_atomicity_tests", "e2e_long_txn_visibility_tests",
            "e2e_keyless_duplicate_tests", "e2e_safety_tests", "e2e_rls_wal_tests",
            "e2e_dvm_corpus_tests", "e2e_dvm_negative_control_tests",
            "e2e_sensitivity_baseline_tests", "e2e_window_state_recovery_tests",
            "e2e_dag_operations_tests", "e2e_dag_concurrent_tests",
            "e2e_monitoring_tests", "e2e_stmt_cdc_tests",
        ):
            self.assertIn(name, full)

    def test_sensitive_cases_are_run_serially_on_both_allocations(self):
        expected = [
            "./scripts/run_e2e_tests.sh", "--profile", "ci", "--retries", "0",
            "--no-fail-fast", "--test-threads", "1", "--test", "e2e_rls_wal_tests",
            "--test", "e2e_wal_receipt_recovery_tests", "--test", "e2e_v043_tests",
            "--test", "e2e_v107_wal_transition_tests", "-E",
            "test(=test_rls_bypassrls_owner_differential_wal_matches_query) | "
            "test(=test_wal_recovery_before_receipt_commit_preserves_committed_changes) | "
            "test(=test_wal_recovery_after_receipt_commit_before_ack_converges_once) | "
            "test(=test_wal_recovery_after_apply_before_ack_does_not_reapply) | "
            "test(=test_wal_recovery_after_slot_advance_finishes_acknowledgement) | "
            "test(=test_wal_trigger_fallback_received_receipt_requires_rebaseline) | "
            "test(=test_wal_trigger_fallback_undecoded_commit_requires_rebaseline) | "
            "test(=test_wal_recovery_semantic_controls_are_detected) | "
            "test(=test_t_a44_10_insert_correctness) | "
            "test(=test_v107_auto_wal_transition_captures_changes)",
        ]
        with patch("sys.argv", ["ci_pr_suite.py"]), patch(
            "ci_pr_suite.subprocess.run", side_effect=[
                SimpleNamespace(returncode=0), SimpleNamespace(returncode=0),
                SimpleNamespace(returncode=0),
            ],
        ) as run:
            self.assertEqual(ci_pr_suite.main(), 0)
        self.assertIn(
            "not (binary(=e2e_rls_wal_tests) & test(=test_rls_bypassrls_owner_differential_wal_matches_query))",
            run.call_args_list[0].args[0][-1],
        )
        self.assertEqual(run.call_args_list[2].args[0], expected)

        before = run_ci_allocation.commands("before", "full")
        after = run_ci_allocation.commands("after", "full")
        self.assertIn("not (binary(=e2e_rls_wal_tests)", before[0][-1])
        self.assertEqual(before[2], expected)
        self.assertEqual(after[0][1:3], ["scripts/ci_pr_suite.py", "--no-fail-fast"])

    def fixture(self, root):
        (root / "tests").mkdir()
        (root / "scripts").mkdir()
        (root / ".github").mkdir()
        (root / "tests/e2e_example_tests.rs").write_text("")
        (root / "scripts/run_light_e2e_tests.sh").write_text("    e2e_example_tests\n")
        (root / ".github/ci-suites.json").write_text(json.dumps({
            "pr_full": [], "scheduled_only": {}, "scheduled_cases": {},
        }))

    def test_new_target_requires_explicit_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            (root / "tests/e2e_new.rs").write_text("")
            with self.assertRaisesRegex(ValueError, "unmapped targets.*e2e_new"):
                ci_pr_suite.targets(root)

    def test_light_conditional_requires_full_harness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            (root / "tests/e2e_example_tests.rs").write_text('#[cfg(not(feature = "light-e2e"))]')
            with self.assertRaisesRegex(ValueError, "full-harness cases omitted"):
                ci_pr_suite.targets(root)

    def test_runner_propagates_test_failure_and_cancellation(self):
        for status in (1, 130):
            with self.subTest(status=status), patch("sys.argv", ["ci_pr_suite.py"]), patch(
                "ci_pr_suite.subprocess.run"
            ) as run:
                run.return_value.returncode = status
                self.assertEqual(ci_pr_suite.main(), status)
                command = run.call_args.args[0]
                self.assertEqual(command[:3], ["./scripts/run_e2e_tests.sh", "--profile", "ci"])
                self.assertNotIn("e2e_*", command)
                self.assertEqual(command[command.index("--retries") + 1], "0")

    def test_atomicity_runs_isolated_without_retries_and_propagates_failure(self):
        for status in (0, 1, 130):
            with self.subTest(status=status), patch("sys.argv", ["ci_pr_suite.py"]), patch(
                "ci_pr_suite.subprocess.run"
            ) as run:
                run.side_effect = [SimpleNamespace(returncode=value) for value in (0, 0, status)]
                self.assertEqual(ci_pr_suite.main(), status)
                self.assertNotIn("e2e_refresh_atomicity_tests", run.call_args_list[0].args[0])
                self.assertEqual(run.call_args_list[1].args[0], [
                    "./scripts/run_e2e_tests.sh", "--profile", "ci", "--test",
                    "e2e_refresh_atomicity_tests", "--test-threads", "1", "--retries", "0",
                ])
                self.assertIn("--test-threads", run.call_args_list[2].args[0])

    def test_comparison_collects_all_selected_failures_without_changing_result(self):
        for status in (0, 1, 130):
            with self.subTest(status=status), patch(
                "sys.argv", ["ci_pr_suite.py", "--no-fail-fast"]
            ), patch("ci_pr_suite.subprocess.run") as run:
                run.return_value.returncode = status
                self.assertEqual(ci_pr_suite.main(), status)
                self.assertIn("--no-fail-fast", run.call_args_list[0].args[0])


class VerdictTests(unittest.TestCase):
    def needs(self, *, runtime=True):
        workflow = (ci_pr_suite.ROOT / ".github/workflows/ci.yml").read_text()
        dependencies = workflow.split("  pr-correctness:\n", 1)[1].split("    runs-on:", 1)[0]
        names = [line.strip()[2:] for line in dependencies.splitlines() if line.startswith("      - ")]
        needs = {name: {"result": "skipped", "outputs": {}} for name in names}
        for name in ("release-qualification-contract", "lsn-verification", "upgrade-check"):
            needs[name]["result"] = "success"
        needs["release-qualification-contract"]["outputs"]["runtime"] = str(runtime).lower()
        needs["detect-e2e-gates"]["outputs"] = {
            "unsafe_boundary_changed": "false", "failure_recovery_changed": "false",
        }
        if runtime:
            for name in ("unit-tests", "integration-tests", "otel-collector-contract"):
                needs[name]["result"] = "success"
        return needs

    def test_docs_and_draft_then_ready_then_draft(self):
        self.assertEqual(check_ci_results.check(self.needs(runtime=False), draft=False), [])
        draft = self.needs()
        self.assertEqual(check_ci_results.check(draft, draft=True), [])
        self.assertIn("light-e2e-tests: skipped", check_ci_results.check(draft, draft=False))
        ready = copy.deepcopy(draft)
        for name in ready:
            if name != "unsafe-boundary-asan":
                ready[name]["result"] = "success"
        self.assertEqual(check_ci_results.check(ready, draft=False), [])
        self.assertEqual(check_ci_results.check(draft, draft=True), [])

    def test_each_selected_job_rejects_failure_cancellation_and_skip(self):
        needs = self.needs()
        for job in needs.values():
            job["result"] = "success"
        needs["detect-e2e-gates"]["outputs"]["unsafe_boundary_changed"] = "true"
        for name in needs:
            for status in ("failure", "cancelled", "skipped"):
                with self.subTest(name=name, status=status):
                    failed = copy.deepcopy(needs)
                    failed[name]["result"] = status
                    self.assertIn(f"{name}: {status}", check_ci_results.check(failed, draft=False))

    def test_shipping_and_monitoring_inputs_select_their_checks(self):
        for output, name in (("shipping", "shipping-image-upgrade"), ("monitoring", "otel-collector-contract")):
            needs = self.needs(runtime=False)
            needs["release-qualification-contract"]["outputs"][output] = "true"
            self.assertIn(f"{name}: skipped", check_ci_results.check(needs, draft=False))

    def test_missing_selection_cannot_pass_as_docs_only(self):
        needs = self.needs(runtime=False)
        del needs["release-qualification-contract"]["outputs"]["runtime"]
        self.assertIn(
            "release-qualification-contract: missing/invalid runtime selection",
            check_ci_results.check(needs, draft=False),
        )


if __name__ == "__main__":
    unittest.main()
