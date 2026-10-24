"""Controls must mutate exactly one production executable decision."""

import unittest

import check_aggregate_lifecycle_verification as lifecycle


class LifecycleMutationTests(unittest.TestCase):
    def test_all_controls_bind_one_executable_decision(self):
        source = lifecycle.SOURCE.read_text()
        self.assertEqual(len(lifecycle.MUTATIONS), 5)
        for name in lifecycle.MUTATIONS:
            mutated = lifecycle.mutate(source, name)
            self.assertNotEqual(mutated, source)
            for stale in ["", source + source]:
                with self.assertRaisesRegex(ValueError, "exactly one executable"):
                    lifecycle.mutate(stale, name)

    def test_named_production_callers_execute_checked_decisions(self):
        lifecycle.check_contract()


if __name__ == "__main__":
    unittest.main()
