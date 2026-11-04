"""Exercise the upgrade slice's actual workflow command with controlled outcomes."""

import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


class UpgradeSliceCommandTests(unittest.TestCase):
    def test_nextest_outcomes_are_preserved(self):
        workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
        step = workflow.split("      - name: Run upgrade E2E slice\n", 1)[1]
        command = step.split("        run: |\n", 1)[1].split("\n      - name:", 1)[0]
        command = textwrap.dedent(command)

        with tempfile.TemporaryDirectory() as directory:
            cargo = Path(directory) / "cargo"
            cargo.write_text(
                '#!/bin/sh\n'
                'case " $* " in *" --no-tests fail "*) ;; *) exit 99;; esac\n'
                'echo "nextest diagnostic: $OUTCOME" >&2\n'
                'exit "$STATUS"\n'
            )
            cargo.chmod(0o755)
            for outcome, status in [("passed", 0), ("assertion failure", 100),
                                    ("build failure", 101), ("empty selection", 4)]:
                with self.subTest(outcome=outcome):
                    env = {**os.environ, "PATH": directory + os.pathsep + os.environ["PATH"],
                           "OUTCOME": outcome, "STATUS": str(status)}
                    result = subprocess.run(["bash", "-e", "-c", command], env=env,
                                            capture_output=True, text=True)
                    self.assertEqual(result.returncode, status, result.stderr)
                    self.assertIn(f"nextest diagnostic: {outcome}", result.stderr)


if __name__ == "__main__":
    unittest.main()
