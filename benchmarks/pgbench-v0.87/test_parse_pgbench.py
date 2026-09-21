#!/usr/bin/env python3
"""Check worker CPU accounting across process exits and PID reuse."""

from pathlib import Path
import runpy
from tempfile import TemporaryDirectory


worker_ticks = runpy.run_path(str(Path(__file__).with_name("parse_pgbench.py")))["worker_ticks"]

with TemporaryDirectory() as directory:
    samples = Path(directory) / "cpu.samples"
    samples.write_text(
        "S\nW 10 100 50\n"
        "S\nW 10 100 55\nW 11 200 3\n"
        "S\nW 11 200 8\nW 10 300 2\n",
        encoding="utf-8",
    )
    assert worker_ticks(samples) == 15
