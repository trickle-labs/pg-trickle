"""The receipt mutant must change exactly the production executable expression."""

import unittest

import check_receipt_verification as receipt


class ReceiptMutationTests(unittest.TestCase):
    def test_mutation_is_bound_to_one_executable_limit(self):
        source = receipt.SOURCE.read_text()
        mutated = receipt.mutate(source)
        self.assertNotEqual(source, mutated)
        self.assertIn("Some((message_count + 1) as i32)", mutated)
        for stale in ["", source + source, mutated]:
            with self.assertRaisesRegex(ValueError, "exactly one executable"):
                receipt.mutate(stale)

    def test_production_import_and_call_remain_bound(self):
        receipt.check_contract()

    def test_bookkeeping_mutations_cannot_use_stale_or_duplicated_sites(self):
        source=receipt.SOURCE.read_text()
        for name in receipt.MUTATIONS:
            changed=receipt.mutate(source,name)
            self.assertNotEqual(source,changed)
            for invalid in ["",source+source,changed]:
                with self.assertRaisesRegex(ValueError,"exactly one executable"):
                    receipt.mutate(invalid,name)


if __name__ == "__main__":
    unittest.main()
