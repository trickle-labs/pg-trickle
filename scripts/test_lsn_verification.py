"""Proof execution stays mandatory while failed-proof artifacts remain available."""

import unittest

import check_lsn_verification as lsn


class ProofJobConditionTests(unittest.TestCase):
    proof = """
    steps:
      - name: Verify
        run: python3 scripts/check_lsn_verification.py
      - name: Retain evidence
        if: always()
        uses: actions/upload-artifact@v7
        with:
          if-no-files-found: error
"""

    def test_actual_workflow_preserves_required_proof_and_failure_evidence(self):
        lsn.check_contract()

    def test_always_upload_does_not_make_execution_conditional(self):
        lsn.check_ci_proof_job(self.proof)

    def test_conditions_cannot_skip_proof_job_or_steps(self):
        for conditional in [
            "    if: false\n" + self.proof,
            self.proof.replace("      - name: Verify\n", "      - name: Verify\n        if: always()\n"),
            self.proof.replace("if: always()", "if: success()"),
            self.proof.replace("      - name: Verify\n", "      - name: Verify\n        continue-on-error: true\n"),
        ]:
            with self.subTest(conditional=conditional):
                with self.assertRaisesRegex(ValueError, "conditionally skipped or ignored"):
                    lsn.check_ci_proof_job(conditional)

    def test_missing_proof_command_is_rejected_even_with_evidence_upload(self):
        with self.assertRaisesRegex(ValueError, "verifier command exactly"):
            lsn.check_ci_proof_job(self.proof.replace("python3 scripts/check_lsn_verification.py", "true"))


if __name__ == "__main__":
    unittest.main()
