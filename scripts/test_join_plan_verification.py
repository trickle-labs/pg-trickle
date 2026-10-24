import unittest

import check_join_plan_verification as join
import check_join_sql_control as sql_control


class JoinPlanTests(unittest.TestCase):
    def test_native_qualification_removes_only_actual_ansi_color_sequences(self):
        self.assertEqual(sql_control.plain_output("\x1b[1;31mFAIL\x1b[0m 1 failed"), "FAIL 1 failed")
        self.assertEqual(sql_control.plain_output("[31m is a SQL value; 1 passed"), "[31m is a SQL value; 1 passed")

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
