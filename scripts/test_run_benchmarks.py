import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class BenchmarkRunnerTests(unittest.TestCase):
    @unittest.skipIf(os.name == "nt", "benchmark runner uses Unix shell and preload")
    def test_uses_cargo_executable_outside_release_deps(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            (project / "scripts").mkdir(parents=True)
            (project / "scripts/pg_stub.c").write_text("")
            target = project / "target"
            target.mkdir()
            stub = target / ("libpg_stub.dylib" if os.uname().sysname == "Darwin" else "libpg_stub.so")
            stub.write_text("")
            bindir = root / "bin"
            bindir.mkdir()
            cargo = bindir / "cargo"
            cargo.write_text('#!/bin/sh\ncat "$FAKE_CARGO_MESSAGES"\nexit "${FAKE_CARGO_STATUS:-0}"\n')
            cargo.chmod(0o755)
            executable = root / "build path" / "diff-bench"
            executable.parent.mkdir()
            executable.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$BENCH_ARGS"\n')
            executable.chmod(0o755)
            messages = root / "messages.jsonl"
            messages.write_text(json.dumps({"reason": "compiler-artifact", "target": {"name": "diff_operators", "kind": ["bench"]}, "executable": str(executable)}) + "\n")
            args_file = root / "args"
            env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}", BENCH_PROJECT_DIR=str(project), CARGO_TARGET_DIR=str(target), FAKE_CARGO_MESSAGES=str(messages), BENCH_ARGS=str(args_file))
            runner = Path(__file__).with_name("run_benchmarks.sh")
            subprocess.run(["bash", str(runner), "diff_operators", "--", "--sample-size", "10"], env=env, check=True, capture_output=True, text=True)
            self.assertEqual(args_file.read_text().splitlines(), ["--bench", "--sample-size", "10"])
            messages.write_text("")
            result = subprocess.run(["bash", str(runner), "diff_operators"], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Could not find benchmark binary", result.stderr)
            messages.write_text(json.dumps({"reason": "compiler-message", "message": {"rendered": "compiler error detail\n"}}) + "\n")
            failed_env = dict(env, FAKE_CARGO_STATUS="7")
            result = subprocess.run(["bash", str(runner), "diff_operators"], env=failed_env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 7)
            self.assertIn("compiler error detail", result.stderr)


if __name__ == "__main__":
    unittest.main()
