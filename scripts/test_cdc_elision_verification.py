import unittest

import check_cdc_elision_verification as cdc


class CdcElisionTests(unittest.TestCase):
    def test_mutants_bind_one_executable_decision(self):
        source = cdc.SOURCE.read_text()
        for name in cdc.MUTATIONS:
            self.assertNotEqual(cdc.mutate(source, name), source)
            for stale in ["", source + source]:
                with self.assertRaisesRegex(ValueError, "exactly one executable"):
                    cdc.mutate(stale, name)

    def test_both_trigger_builders_execute_the_policy(self):
        cdc.check_contract()


if __name__ == "__main__":
    unittest.main()
