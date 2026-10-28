import unittest

import check_join_plan_verification as join


class JoinPlanTests(unittest.TestCase):
    def test_mutants_fail_closed_when_the_executable_source_moves(self):
        source = join.SOURCE.read_text()
        for name in join.MUTATIONS:
            self.assertNotEqual(join.mutate(source, name), source)
            for stale in ["", source + source]:
                with self.assertRaisesRegex(ValueError, "exactly one executable"):
                    join.mutate(stale, name)

    def test_actual_production_adapter_uses_the_checked_plan(self):
        join.check_contract()


if __name__ == "__main__":
    unittest.main()
