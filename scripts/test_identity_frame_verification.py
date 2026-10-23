"""Fail-closed contract checks for production identity proof evidence."""

import unittest

import check_identity_frame_verification as frame


class IdentityFrameTests(unittest.TestCase):
    def test_executable_controls_reject_missing_or_duplicate_decisions(self):
        source = frame.SOURCE.read_text()
        for name in frame.MUTATIONS:
            self.assertNotEqual(frame.mutate(source, name), source)
            for stale in ["", source + source]:
                with self.assertRaisesRegex(ValueError, "exactly one executable"):
                    frame.mutate(stale, name)

    def test_normal_postgresql_and_pure_adapters_use_checked_functions(self):
        frame.check_contract()


if __name__ == "__main__":
    unittest.main()
