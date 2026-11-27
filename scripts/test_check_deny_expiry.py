"""Regression tests for the shared advisory ignore and expiry policy."""
import contextlib
import io
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import check_deny_expiry as policy


def config(entries: str) -> str:
    return '[advisories]\nversion = 2\nignore = [\n' + entries + '\n]\n'


class AdvisoryPolicyTests(unittest.TestCase):
    def test_long_rationale_binds_deadline_to_next_entry(self):
        text = config(
            '# Review-By: 2026-12-08\n"RUSTSEC-2026-0440",\n'
            '# Review-By: 2026-12-24\n' + '# rationale\n' * 15
            + '"RUSTSEC-2026-0543",'
        )
        self.assertEqual(policy.review_dates(text), {
            'RUSTSEC-2026-0440': date(2026, 12, 8),
            'RUSTSEC-2026-0543': date(2026, 12, 24),
        })

    def test_missing_review_fails(self):
        with self.assertRaisesRegex(ValueError, 'no Review-By'):
            policy.review_dates(config('"RUSTSEC-2026-0543",'))

    def test_duplicate_review_fails(self):
        with self.assertRaisesRegex(ValueError, 'duplicate Review-By'):
            policy.review_dates(config(
                '# Review-By: 2026-12-01\n# Review-By: 2026-12-02\n'
                '"RUSTSEC-2026-0543",'
            ))

    def test_duplicate_id_fails(self):
        entry = '# Review-By: 2026-12-24\n"RUSTSEC-2026-0543",\n'
        with self.assertRaisesRegex(ValueError, 'duplicate advisory'):
            policy.review_dates(config(entry * 2))

    def test_expiry_and_inclusive_review_day(self):
        text = config('# Review-By: 2026-12-08\n"RUSTSEC-2026-0440",')
        self.assertEqual(list(policy.validated_ignores(text, date(2026, 12, 8))),
                         ['RUSTSEC-2026-0440'])
        with self.assertRaisesRegex(ValueError, 'RUSTSEC-2026-0440'):
            policy.validated_ignores(text, date(2026, 12, 9))

    def test_empty_policy(self):
        self.assertEqual(policy.review_dates('[advisories]\nignore = []\n'), {})

    def test_export_only_after_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'deny.toml'
            with patch.object(policy, 'DENY_TOML', path):
                path.write_text(config(
                    '# Review-By: 2099-12-31\n"RUSTSEC-2026-0440",\n'
                    '# Review-By: 2099-12-31\n"RUSTSEC-2026-0543",'
                ), encoding='utf-8')
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    self.assertEqual(policy.main(['--audit-ignores']), 0)
                self.assertEqual(out.getvalue(), 'RUSTSEC-2026-0440,RUSTSEC-2026-0543\n')
                path.write_text(config('# Review-By: 2000-01-01\n"RUSTSEC-2026-0543",'),
                                encoding='utf-8')
                out, err = io.StringIO(), io.StringIO()
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                    self.assertEqual(policy.main(['--audit-ignores']), 1)
                self.assertEqual(out.getvalue(), '')
                self.assertIn('RUSTSEC-2026-0543', err.getvalue())


if __name__ == '__main__':
    unittest.main()
