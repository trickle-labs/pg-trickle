#!/usr/bin/env python3
"""Check worker CPU accounting across process exits and PID reuse."""

from pathlib import Path
import runpy
from tempfile import TemporaryDirectory


parser = runpy.run_path(str(Path(__file__).with_name("parse_pgbench.py")))
worker_ticks = parser["worker_ticks"]
read_latencies = parser["read_latencies"]

with TemporaryDirectory() as directory:
    samples = Path(directory) / "cpu.samples"
    samples.write_text(
        "S\nW 10 100 50\n"
        "S\nW 10 100 55\nW 11 200 3\n"
        "S\nW 11 200 8\nW 10 300 2\n",
        encoding="utf-8",
    )
    assert worker_ticks(samples) == 15

with TemporaryDirectory() as directory:
    logs = Path(directory)
    (logs / "plain").write_text("0 1 1346 0 1794818911 470592\n", encoding="utf-8")
    (logs / "rate").write_text("0 1 337079 0 1794819319 52161 335453\n", encoding="utf-8")
    assert read_latencies(logs) == [1.346, 1.626]
