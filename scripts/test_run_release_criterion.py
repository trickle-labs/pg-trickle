import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import run_release_criterion as criterion


class ReleaseCriterionTests(unittest.TestCase):
    def test_baseline_matches_previous_release_in_current_contract(self) -> None:
        criterion.configure_release("0.109.0")
        self.assertEqual(criterion.BASELINE_TAG, "v0.108.3")
        self.assertEqual(criterion.BASELINE_VERSION, "0.108.3")
        self.assertEqual(criterion.QUALIFICATION.name, "v0.109.0-qualification.json")
        contract = json.loads(criterion.QUALIFICATION.read_text(encoding="utf-8"))
        suite = next(s for s in contract["required_suites"] if s["id"] == "criterion-regression")
        self.assertIn(f"v{criterion.BASELINE_VERSION}", suite["workload"])

    def test_missing_contract_does_not_fall_back_to_an_older_release(self) -> None:
        with self.assertRaises(FileNotFoundError):
            criterion.configure_release("999.0.0")

    def test_baseline_uses_current_runner_with_baseline_project(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            contract = root / "tests/release/v0.109.0-qualification.json"
            contract.parent.mkdir(parents=True)
            contract.write_text(json.dumps({"source_versions": ["0.108.3"], "performance_budgets": [{"id": "criterion-regression", "threshold": 10, "minimum_absolute_delta_ns": 250}]}))
            measurement = root / "results/criterion.json"
            measurement.parent.mkdir()
            calls = []

            def run(command, *, cwd, env=None):
                calls.append((command, cwd, env))
                if command[:2] == ["git", "worktree"]:
                    Path(command[-2]).mkdir()
                elif command[0] == "bash":
                    name = "0.108.3" if "--save-baseline" in command else "new"
                    (cwd / "target/criterion/example" / name).mkdir(parents=True)
                else:
                    measurement.write_text("{}")

            with patch.object(criterion, "ROOT", root), patch.object(criterion, "run", side_effect=run), patch.object(criterion.subprocess, "run"), patch.dict(os.environ, {"PGS_RELEASE_MEASUREMENT_JSON": str(measurement)}):
                self.assertEqual(criterion.main(), 0)
            baseline = next(call for call in calls if "--save-baseline" in call[0])
            self.assertEqual(baseline[0][1], str(root / "scripts/run_benchmarks.sh"))
            self.assertEqual(baseline[2]["BENCH_PROJECT_DIR"], str(baseline[1]))


if __name__ == "__main__":
    unittest.main()
