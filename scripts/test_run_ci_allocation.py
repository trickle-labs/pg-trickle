import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import run_ci_allocation


class AllocationReplayTests(unittest.TestCase):
    def test_light_work_is_identical_for_both_allocations(self):
        for tier in ('light-1', 'light-2', 'light-3'):
            self.assertEqual(run_ci_allocation.commands('before', tier),
                             run_ci_allocation.commands('after', tier))

    def test_components_are_only_added_to_after_integration(self):
        before = run_ci_allocation.commands('before', 'integration')
        after = run_ci_allocation.commands('after', 'integration')
        self.assertEqual(after[:-1], before)
        self.assertEqual(after[-1], ['bash', 'scripts/run_dvm_integration_tests.sh', 'pg18'])

    def test_full_allocation_keeps_identical_serial_and_recovery_protection(self):
        before = run_ci_allocation.commands('before', 'full')
        after = run_ci_allocation.commands('after', 'full')
        self.assertIn('e2e_*', before[0])
        expression = before[0][before[0].index('-E') + 1]
        self.assertIn('not binary(=e2e_refresh_atomicity_tests)', expression)
        self.assertIn('test_rls_bypassrls_owner_differential_wal_matches_query', expression)
        self.assertIn('test_wal_trigger_fallback_undecoded_commit_requires_rebaseline', expression)
        self.assertIn('e2e_refresh_atomicity_tests', before[1])
        self.assertIn('--test-threads', before[2])
        serial_expression = before[2][before[2].index('-E') + 1]
        self.assertIn('test(=test_rls_bypassrls_owner_differential_wal_matches_query)', serial_expression)
        self.assertIn('test(=test_wal_trigger_fallback_undecoded_commit_requires_rebaseline)', serial_expression)
        self.assertIn('test(=test_stddev_samp_differential_mode)', serial_expression)
        self.assertIn('test(=test_v0878_revoked_source_preserves_state_and_stages)', serial_expression)
        self.assertIn('test(=test_autorefresh_no_spurious_3_layer)', serial_expression)
        self.assertEqual(after[0], ['python3', 'scripts/ci_pr_suite.py', '--no-fail-fast'])
        self.assertIn('--no-fail-fast', before[0])
        self.assertEqual(before[-1], after[-1])

    def test_failed_step_is_retained_and_does_not_skip_later_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / 'comparison-inputs'
            inputs.mkdir()
            for name in ('package.sha256', 'images.sha256'):
                (inputs / name).write_text('fixed digest\n')
            with patch.object(run_ci_allocation, 'ROOT', root), patch.object(
                run_ci_allocation, 'output', return_value='fixed tool/source'
            ), patch('sys.argv', ['run_ci_allocation.py', 'after', 'integration']), patch.dict(
                run_ci_allocation.os.environ, {'CI_ALLOCATION_CONTAINER': 'sha256:fixed'}
            ), patch.object(run_ci_allocation.subprocess, 'run') as run:
                run.side_effect = [SimpleNamespace(returncode=1), SimpleNamespace(returncode=0)]
                self.assertEqual(run_ci_allocation.main(), 1)
            evidence = root / 'target/allocation-comparison'
            steps = json.loads((evidence / 'steps.json').read_text())
            self.assertEqual([step['exit_code'] for step in steps], [1, 0])
            self.assertEqual(run.call_count, 2)
            self.assertEqual(json.loads((evidence / 'identity.json').read_text())['retries'], 0)

    def test_warm_build_directory_rejects_the_comparison_before_running(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'target/debug').mkdir(parents=True)
            with patch.object(run_ci_allocation, 'ROOT', root), patch(
                'sys.argv', ['run_ci_allocation.py', 'before', 'full']
            ), patch.dict(run_ci_allocation.os.environ):
                with self.assertRaisesRegex(RuntimeError, 'cold workspace'):
                    run_ci_allocation.main()

    def test_native_host_replay_is_rejected_before_running(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(run_ci_allocation, 'ROOT', Path(directory)), patch(
                'sys.argv', ['run_ci_allocation.py', 'before', 'full']
            ), patch.dict(run_ci_allocation.os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, 'frozen toolchain container'):
                    run_ci_allocation.main()


if __name__ == '__main__':
    unittest.main()
