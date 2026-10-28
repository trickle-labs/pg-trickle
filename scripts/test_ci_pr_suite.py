"""Guard the PR allocation, runner invocation, and terminal merge verdict."""

import copy
import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import check_ci_results
import check_lsn_verification
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
            "e2e_monitoring_tests", "e2e_stmt_cdc_tests", "e2e_ownership_tests",
            "e2e_scheduler_tests", "e2e_dvm_failpoint_tests",
        ):
            self.assertIn(name, full)

    def test_sensitive_cases_are_run_serially_on_both_allocations(self):
        expected = [
            "./scripts/run_e2e_tests.sh", "--profile", "ci", "--retries", "0",
            "--no-fail-fast", "--test-threads", "1",
            "--test", "e2e_wal_receipt_recovery_tests", "--test", "e2e_v043_tests",
            "--test", "e2e_lsn_contract_tests", "--test", "e2e_property_circular_tests",
            "--test", "e2e_refresh_tests", "--test", "e2e_v0878_security_tests", "-E",
            "test(=test_wal_recovery_before_receipt_commit_preserves_committed_changes) | "
            "test(=test_wal_recovery_after_receipt_commit_before_ack_converges_once) | "
            "test(=test_wal_recovery_after_apply_before_ack_does_not_reapply) | "
            "test(=test_wal_recovery_after_slot_advance_finishes_acknowledgement) | "
            "test(=test_wal_trigger_fallback_received_receipt_requires_rebaseline) | "
            "test(=test_wal_trigger_fallback_undecoded_commit_requires_rebaseline) | "
            "test(=test_wal_recovery_semantic_controls_are_detected) | "
            "test(=test_t_a44_10_insert_correctness) | "
            "test(=test_lsn_manual_refresh_fixed_snapshots_preserve_frontier) | "
            "test(=test_lsn_scheduler_cdc_holdback_recovers_after_long_writer) | "
            "test(=test_prop_monotone_cycle_small_graph_converges) | "
            "test(=test_stddev_samp_differential_mode) | "
            "test(=test_v0878_revoked_source_preserves_state_and_stages)",
        ]
        with patch("sys.argv", ["ci_pr_suite.py"]), patch(
            "ci_pr_suite.subprocess.run", return_value=SimpleNamespace(returncode=0),
        ) as run:
            self.assertEqual(ci_pr_suite.main(), 0)
        serial = next(call.args[0] for call in run.call_args_list if "--no-fail-fast" in call.args[0])
        self.assertIn(
            "not (binary(=e2e_wal_receipt_recovery_tests) & test(=test_wal_recovery_before_receipt_commit_preserves_committed_changes))",
            run.call_args_list[0].args[0][-1],
        )
        self.assertIn(
            "not (binary(=e2e_refresh_tests) & test(=test_stddev_samp_differential_mode))",
            run.call_args_list[0].args[0][-1],
        )
        self.assertEqual(serial, expected)

        before = run_ci_allocation.commands("before", "full")
        after = run_ci_allocation.commands("after", "full")
        self.assertIn("not (binary(=e2e_wal_receipt_recovery_tests)", before[0][-1])
        self.assertEqual(before[2], run_ci_allocation.serial_case_command())
        self.assertEqual(after[0][1:3], ["scripts/ci_pr_suite.py", "--no-fail-fast"])

    def fixture(self, root):
        (root / "tests").mkdir()
        (root / "scripts").mkdir()
        (root / ".github").mkdir()
        (root / "tests/e2e_example_tests.rs").write_text("")
        (root / "scripts/run_light_e2e_tests.sh").write_text("    e2e_example_tests\n")
        (root / ".github/ci-suites.json").write_text(json.dumps({
            "pr_full": [], "pr_full_shard_2": [], "pr_pure": [],
            "scheduled_only": {}, "scheduled_cases": {},
        }))

    def test_shards_cover_each_target_once_and_keep_witnesses_isolated(self):
        self.assertTrue({
            "e2e_drain_mode_tests", "e2e_long_txn_visibility_tests",
        } <= ci_pr_suite.SCHEDULER_ISOLATED)
        shards = []
        policy = json.loads((ci_pr_suite.ROOT / ".github/ci-suites.json").read_text())
        shard_2 = set(policy["pr_full_shard_2"])
        all_targets = set(ci_pr_suite.targets())
        for shard in (1, 2):
            with patch("sys.argv", ["ci_pr_suite.py", "--shard", str(shard)]), patch(
                "ci_pr_suite.subprocess.run", return_value=SimpleNamespace(returncode=0)
            ) as run:
                self.assertEqual(ci_pr_suite.main(), 0)
                commands = [call.args[0] for call in run.call_args_list]
                selected = {
                    command[index + 1]
                    for command in commands
                    for index, arg in enumerate(command[:-1])
                    if arg == "--test"
                }
                expected = {name for name in all_targets if (name in shard_2) == (shard == 2)}
                self.assertEqual(selected, expected)
                for command in commands:
                    self.assertEqual(command[command.index("--retries") + 1], "0")
                scheduler_runs = {
                    command[command.index("--test") + 1]
                    for command in commands[1:]
                    if "--test-threads" in command and "--no-fail-fast" not in command
                    and command[command.index("--test") + 1] in ci_pr_suite.SCHEDULER_ISOLATED
                }
                self.assertEqual(scheduler_runs, expected & ci_pr_suite.SCHEDULER_ISOLATED)
                serial = [command for command in commands if "--no-fail-fast" in command]
                self.assertEqual(len(serial), 1)
                self.assertIn("--test-threads", serial[0])
                serial_cases = [
                    (binary, test) for binary, test in ci_pr_suite.SERIAL_CASES
                    if binary in expected and binary not in ci_pr_suite.SCHEDULER_ISOLATED
                ]
                self.assertEqual(
                    serial[0][serial[0].index("-E") + 1],
                    " | ".join(f"test(={test})" for _, test in serial_cases),
                )
                shards.append(selected)
        self.assertEqual(shards[0] | shards[1], all_targets)
        self.assertFalse(set(shards[0]) & set(shards[1]))
        for name in ("e2e_refresh_atomicity_tests", "e2e_failure_recovery_tests", "e2e_dvm_failpoint_tests"):
            self.assertIn(name, shards[0])

    def test_invalid_shard_allocation_fails_closed(self):
        for allocation in (["e2e_missing"], ["e2e_refresh_atomicity_tests"],
                           ["e2e_one", "e2e_one"], [], ["e2e_one", "e2e_refresh_atomicity_tests"]):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.fixture(root)
                path = root / ".github/ci-suites.json"
                policy = json.loads(path.read_text())
                policy.update(pr_full=["e2e_one", "e2e_refresh_atomicity_tests"], pr_full_shard_2=allocation)
                path.write_text(json.dumps(policy))
                with self.assertRaisesRegex(ValueError, "invalid full-suite shard"):
                    ci_pr_suite.targets(root)

    def test_both_shards_propagate_failure_and_cancellation(self):
        for shard in (1, 2):
            for status in (1, 130):
                with patch("sys.argv", ["ci_pr_suite.py", "--shard", str(shard)]), patch(
                    "ci_pr_suite.subprocess.run", return_value=SimpleNamespace(returncode=status)
                ) as run:
                    self.assertEqual(ci_pr_suite.main(), status)
                    self.assertEqual(run.call_count, 1)

    def test_repetition_has_pr_regressions_and_executable_nightly_destination(self):
        policy = json.loads((ci_pr_suite.ROOT / ".github/ci-suites.json").read_text())
        self.assertIn("failure_recovery", policy["scheduled_repetitions"])
        runner = (ci_pr_suite.ROOT / "scripts/run_failure_recovery_repetition.sh").read_text()
        cases = re.findall(r"test\((test_\w+)\)", runner)
        self.assertEqual(len(set(cases)), 13)
        sources = "\n".join((ci_pr_suite.ROOT / "tests" / f"{name}.rs").read_text() for name in (
            "e2e_failure_recovery_tests", "e2e_dvm_failpoint_tests", "e2e_refresh_atomicity_tests",
        ))
        for case in cases:
            self.assertIn(f"fn {case}(", sources)
        workflow = (ci_pr_suite.ROOT / ".github/workflows/ci.yml").read_text()
        pr = workflow.split("  e2e-sensitive-gate:\n")[1].split("  pr-correctness:")[0]
        self.assertIn("fail-fast: false", pr)
        self.assertIn("shard: [1, 2]", pr)
        self.assertIn("ci_pr_suite.py --shard ${{ matrix.shard }}", pr)
        self.assertIn("matrix.shard == 1 && steps.full-image.outcome == 'success'", pr)
        self.assertIn("FAILURE_RECOVERY_ATTEMPTS: '2'", pr)
        nightly = workflow.split("  production-recovery-qualification:\n")[1].split("  unsafe-boundary-asan:")[0]
        self.assertIn("github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'", nightly)
        self.assertIn("cron: '0 3 * * *'", workflow)
        self.assertIn("FAILURE_RECOVERY_ATTEMPTS: '10'", nightly)
        self.assertIn("bash ./scripts/run_failure_recovery_repetition.sh", nightly)
        self.assertIn("name: failure-recovery-repetition-nightly", nightly)
        self.assertIn("if-no-files-found: error", nightly)
        self.assertIn("always() && steps.recovery-image.outcome == 'success'", nightly)

    def test_pure_tier_rejects_installed_tests_and_conflicting_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            policy_path = root / ".github/ci-suites.json"
            policy = json.loads(policy_path.read_text())
            policy["pr_pure"] = ["e2e_example_tests"]
            policy_path.write_text(json.dumps(policy))
            with self.assertRaisesRegex(ValueError, "conflicting"):
                ci_pr_suite.targets(root)
            (root / "scripts/run_light_e2e_tests.sh").write_text("")
            (root / "tests/e2e_example_tests.rs").write_text("#[tokio::test]")
            with self.assertRaisesRegex(ValueError, "database test allocated to pure tier"):
                ci_pr_suite.targets(root)

    def test_pure_runner_and_failure_propagation(self):
        for status in (0, 1, 130):
            with self.subTest(status=status), patch("sys.argv", ["ci_pr_suite.py", "--pure"]), patch(
                "ci_pr_suite.subprocess.run"
            ) as run:
                run.return_value.returncode = status
                self.assertEqual(ci_pr_suite.main(), status)
                self.assertEqual(run.call_args.args[0], [
                    "cargo", "test", "--features", "pg18", "--test", "e2e_dvm_fuzz_tests",
                    "--", "--test-threads=1",
                ])

    def test_proof_dependencies_and_consolidated_sensitive_selection(self):
        workflow = (ci_pr_suite.ROOT / ".github/workflows/ci.yml").read_text()
        package = workflow.split("  light-e2e-package:\n")[1].split("  release-evidence-runtime:")[0]
        self.assertIn("needs: [release-qualification-contract]", package)
        self.assertIn("needs.release-qualification-contract.outputs.runtime", package)
        candidate = workflow.split("  lsn-candidate-runtime:\n")[1].split("    steps:")[0]
        self.assertIn("needs: [light-e2e-package, release-qualification-contract]", candidate)
        unit = workflow.split("  unit-tests:\n")[1].split("  otel-collector-contract:")[0]
        self.assertIn("python3 scripts/ci_pr_suite.py --pure", unit)
        self.assertNotIn("detect-e2e-gates", workflow)
        for name in ("e2e-sensitive-gate", "unsafe-boundary-asan"):
            gate = workflow.split(f"  {name}:\n")[1].split("    runs-on:")[0]
            self.assertIn("needs: release-qualification-contract", gate)
            self.assertIn("!github.event.pull_request.draft", gate)
            self.assertIn("needs.release-qualification-contract.outputs.runtime == 'true'", gate)
        smoke = workflow.split("  e2e-smoke:\n")[1].split("    timeout-minutes:")[0]
        self.assertIn("github.event_name == 'push'", smoke)
        self.assertIn("needs.release-qualification-contract.outputs.runtime == 'true'", smoke)
        paths = (ci_pr_suite.ROOT / ".github/ci-paths.yml").read_text()
        self.assertIn("'scripts/ci_prepare_disk.sh'", paths.split("harness:\n")[1].split("benchmarks:\n")[0])
        self.assertIn("'scripts/ci_prepare_disk.sh'", paths.split("failure_recovery_changed:\n")[1].split("unsafe_boundary_changed:\n")[0])

    def test_proof_consolidation_cannot_skip_verification_or_ungate_package(self):
        workflow = (ci_pr_suite.ROOT / ".github/workflows/ci.yml").read_text()
        check_lsn_verification.check_ci_contract(workflow)
        for old, new in (
            ("  release-qualification-contract:\n", "  release-qualification-contract:\n    if: false\n"),
            ("  release-qualification-contract:\n", "  release-qualification-contract:\n    continue-on-error: true\n"),
            ("      - name: Run pinned Verus baseline and mutation control\n",
             "      - name: Run pinned Verus baseline and mutation control\n        if: false\n"),
            ("      - name: Run pinned Verus baseline and mutation control\n",
             "      - name: Run pinned Verus baseline and mutation control\n        continue-on-error: true\n"),
            ("run: python3 scripts/check_lsn_verification.py\n", "run: python3 scripts/check_lsn_verification.py --contract-only\n"),
            ("needs: [release-qualification-contract]", "needs: []"),
            ("    if: needs.release-qualification-contract.outputs.runtime == 'true' && (github.event_name != 'pull_request'",
             "    if: always() && needs.release-qualification-contract.outputs.runtime == 'true' && (github.event_name != 'pull_request'"),
        ):
            with self.subTest(mutation=new), self.assertRaises(ValueError):
                self.assertIn(old, workflow)
                check_lsn_verification.check_ci_contract(workflow.replace(old, new))

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

    def test_isolated_scheduler_failure_propagates_without_retries(self):
        for failed_phase in (1, 2, 3):
            with self.subTest(failed_phase=failed_phase), patch(
                "sys.argv", ["ci_pr_suite.py", "--shard", "2"]
            ), patch("ci_pr_suite.subprocess.run") as run:
                run.side_effect = [
                    SimpleNamespace(returncode=value)
                    for value in ([0] * failed_phase + [130])
                ]
                self.assertEqual(ci_pr_suite.main(), 130)
                self.assertEqual(run.call_count, failed_phase + 1)
                command = run.call_args.args[0]
                self.assertEqual(command[command.index("--test-threads") + 1], "1")
                self.assertEqual(command[command.index("--retries") + 1], "0")

    def test_atomicity_runs_isolated_without_retries_and_propagates_failure(self):
        for status in (0, 1, 130):
            with self.subTest(status=status), patch("sys.argv", ["ci_pr_suite.py"]), patch(
                "ci_pr_suite.subprocess.run"
            ) as run:
                atomicity_index = 1 + len(ci_pr_suite.SCHEDULER_ISOLATED)
                run.side_effect = [
                    SimpleNamespace(returncode=value)
                    for value in ([0] * atomicity_index + [status, 0])
                ]
                self.assertEqual(ci_pr_suite.main(), status)
                self.assertNotIn("e2e_refresh_atomicity_tests", run.call_args_list[0].args[0])
                self.assertEqual(len(run.call_args_list), atomicity_index + 2 if status == 0 else atomicity_index + 1)
                self.assertEqual(run.call_args_list[atomicity_index].args[0], [
                    "./scripts/run_e2e_tests.sh", "--profile", "ci", "--test",
                    "e2e_refresh_atomicity_tests", "--test-threads", "1", "--retries", "0",
                ])
                if status == 0:
                    self.assertIn("--no-fail-fast", run.call_args_list[atomicity_index + 1].args[0])


class RepetitionTests(unittest.TestCase):
    def test_every_attempt_is_retained_and_a_later_pass_cannot_hide_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            runner = root / "scripts/run_e2e_tests.sh"
            runner.write_text(
                '#!/bin/bash\n'
                'printf "%s\\n" "$*" >> calls\n'
                'if [ "$(wc -l < calls)" -eq 1 ]; then exit 1; fi\n'
            )
            runner.chmod(0o755)
            result = subprocess.run(
                ["bash", str(ci_pr_suite.ROOT / "scripts/run_failure_recovery_repetition.sh")],
                cwd=root, capture_output=True, text=True,
                env=dict(os.environ, FAILURE_RECOVERY_ATTEMPTS="2",
                         FAILURE_RECOVERY_ARTIFACT_DIR=str(root / "witnesses")),
            )
            self.assertEqual(result.returncode, 1, result.stderr)
            summary = (root / "witnesses/summary.tsv").read_text().splitlines()
            self.assertEqual([line.split("\t")[:2] for line in summary[1:]], [["1", "1"], ["2", "0"]])
            for attempt in (1, 2):
                self.assertTrue((root / f"witnesses/attempt-{attempt}.log").exists())
            calls = (root / "calls").read_text().splitlines()
            self.assertEqual(len(calls), 2)
            for call in calls:
                self.assertIn("--test-threads 1", call)
                self.assertIn("--retries 0", call)
                self.assertIn("test_refresh_caller_rollback_preserves_committed_state", call)

    def test_nonpositive_or_invalid_repetition_count_cannot_pass(self):
        for attempts in ("0", "-1", "abc", "1.5"):
            result = subprocess.run(
                ["bash", str(ci_pr_suite.ROOT / "scripts/run_failure_recovery_repetition.sh")],
                capture_output=True, text=True,
                env=dict(os.environ, FAILURE_RECOVERY_ATTEMPTS=attempts),
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("positive integer", result.stderr)


class DiskPreparationTests(unittest.TestCase):
    def test_ample_space_skips_cleanup_and_tight_space_retains_it(self):
        for available, cleanup in ((87 * 1024 * 1024, False), (60 * 1024 * 1024, False), (10 * 1024 * 1024, True)):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for name, body in {
                    "df": f'printf "Filesystem Blocks Used Available Capacity Mounted\\nroot 100 0 {available} 0 /\\n"\n',
                    "sudo": 'printf "%s\\n" "$*" > cleanup\n',
                }.items():
                    path = root / name
                    path.write_text("#!/bin/bash\n" + body)
                    path.chmod(0o755)
                result = subprocess.run(
                    ["bash", str(ci_pr_suite.ROOT / "scripts/ci_prepare_disk.sh")],
                    cwd=root, capture_output=True, text=True,
                    env=dict(os.environ, PATH=f"{root}:/usr/bin:/bin", GITHUB_ACTIONS="true",
                             RUNNER_OS="Linux", AGENT_TOOLSDIRECTORY="/runner/tools"),
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual((root / "cleanup").exists(), cleanup)
                if cleanup:
                    self.assertEqual((root / "cleanup").read_text().strip(),
                                     "rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc /usr/local/share/boost /runner/tools")

    def test_local_machine_is_not_cleaned(self):
        result = subprocess.run(
            ["bash", str(ci_pr_suite.ROOT / "scripts/ci_prepare_disk.sh")],
            capture_output=True, text=True, env=dict(os.environ, GITHUB_ACTIONS="false"),
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("only supported", result.stderr)


    def test_atomicity_runs_isolated_without_retries_and_propagates_failure(self):
        for status in (0, 1, 130):
            with self.subTest(status=status), patch("sys.argv", ["ci_pr_suite.py"]), patch(
                "ci_pr_suite.subprocess.run"
            ) as run:
                atomicity_index = 1 + len(ci_pr_suite.SCHEDULER_ISOLATED)
                run.side_effect = [
                    SimpleNamespace(returncode=value)
                    for value in ([0] * atomicity_index + [status, 0])
                ]
                self.assertEqual(ci_pr_suite.main(), status)
                self.assertNotIn("e2e_refresh_atomicity_tests", run.call_args_list[0].args[0])
                self.assertEqual(len(run.call_args_list), atomicity_index + 2 if status == 0 else atomicity_index + 1)
                self.assertEqual(run.call_args_list[atomicity_index].args[0], [
                    "./scripts/run_e2e_tests.sh", "--profile", "ci", "--test",
                    "e2e_refresh_atomicity_tests", "--test-threads", "1", "--retries", "0",
                ])
                if status == 0:
                    self.assertIn("--no-fail-fast", run.call_args_list[atomicity_index + 1].args[0])

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
        for name in ("release-qualification-contract", "upgrade-check"):
            needs[name]["result"] = "success"
        needs["release-qualification-contract"]["outputs"]["runtime"] = str(runtime).lower()
        needs["release-qualification-contract"]["outputs"].update({
            "unsafe_boundary_changed": "false", "failure_recovery_changed": "false",
        })
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
        needs["release-qualification-contract"]["outputs"]["unsafe_boundary_changed"] = "true"
        for name in needs:
            for status in ("failure", "cancelled", "skipped", "missing"):
                with self.subTest(name=name, status=status):
                    failed = copy.deepcopy(needs)
                    failed[name]["result"] = status
                    self.assertIn(f"{name}: {status}", check_ci_results.check(failed, draft=False))
            missing = copy.deepcopy(needs)
            del missing[name]
            self.assertIn(f"{name}: missing", check_ci_results.check(missing, draft=False))

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

    def test_missing_sensitive_selection_cannot_skip_qualification(self):
        for flag in ("unsafe_boundary_changed", "failure_recovery_changed"):
            needs = self.needs()
            del needs["release-qualification-contract"]["outputs"][flag]
            self.assertIn(
                f"release-qualification-contract: missing/invalid {flag}",
                check_ci_results.check(needs, draft=False),
            )


if __name__ == "__main__":
    unittest.main()
