import copy
import json
from pathlib import Path
import tempfile
import unittest

import compare_ci_allocations


class PairedReportTests(unittest.TestCase):
    def fixture(self, root):
        run = {'status': 'completed', 'conclusion': 'success', 'head_sha': 'fixed-sha',
               'html_url': 'https://example.test/run', 'event': 'workflow_dispatch'}
        jobs = []
        for allocation in ('before', 'after'):
            for tier in sorted(compare_ci_allocations.TIERS):
                jobs.append({
                    'name': f'allocation-comparison / Replay ({allocation}, {tier})',
                    'conclusion': 'success', 'created_at': '2026-10-19T00:00:00Z',
                    'started_at': '2026-10-19T00:01:00Z',
                    'completed_at': '2026-10-19T00:03:00Z',
                })
                folder = root / f'allocation-{allocation}-{tier}'
                folder.mkdir()
                identity = {field: 'fixed' for field in compare_ci_allocations.IDENTITY_FIELDS}
                identity.update(allocation=allocation, tier=tier, retries=0, build_cache='disabled',
                                comparison_sha=run['head_sha'])
                (folder / 'identity.json').write_text(json.dumps(identity))
                (folder / 'steps.json').write_text(json.dumps([{'exit_code': 0, 'seconds': 60}]))
        return run, jobs

    def test_complete_matched_pair_has_five_jobs_per_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run, jobs = self.fixture(root)
            report = compare_ci_allocations.compare(run, jobs, root)
            self.assertTrue(report['valid'], report['errors'])
            self.assertEqual(report['allocations']['before']['job_count'], 5)
            self.assertEqual(report['allocations']['after']['execution_minutes'], 10)
            self.assertEqual(report['allocations']['after']['workload_minutes'], 5)

    def test_different_source_digest_cannot_produce_a_speedup_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run, jobs = self.fixture(root)
            path = root / 'allocation-after-full/identity.json'
            identity = json.loads(path.read_text())
            identity['sources'] = 'different sources'
            path.write_text(json.dumps(identity))
            report = compare_ci_allocations.compare(run, jobs, root)
            self.assertFalse(report['valid'])
            self.assertNotIn('observed_execution_minutes_saved', report)

    def test_failure_cancellation_and_missing_replay_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run, jobs = self.fixture(root)
            for result in ('failure', 'cancelled'):
                changed = copy.deepcopy(jobs)
                changed[0]['conclusion'] = result
                report = compare_ci_allocations.compare(run, changed, root)
                self.assertFalse(report['valid'])
                self.assertNotIn('observed_execution_minutes_saved', report)
            self.assertFalse(compare_ci_allocations.compare(run, jobs[:-1], root)['valid'])
            run['status'] = 'in_progress'
            with self.assertRaisesRegex(ValueError, 'complete'):
                compare_ci_allocations.compare(run, jobs, root)

    def test_uniform_artifacts_from_another_run_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run, jobs = self.fixture(root)
            for path in root.glob('*/identity.json'):
                identity = json.loads(path.read_text())
                identity['comparison_sha'] = 'another-run'
                path.write_text(json.dumps(identity))
            report = compare_ci_allocations.compare(run, jobs, root)
            self.assertFalse(report['valid'])
            self.assertTrue(any('different commit' in error for error in report['errors']))
            self.assertNotIn('observed_execution_minutes_saved', report)


if __name__ == '__main__':
    unittest.main()
