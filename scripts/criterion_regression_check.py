#!/usr/bin/env python3
"""
criterion_regression_check.py — Criterion performance regression gate.

Walks the target/criterion/ directory tree and compares each benchmark's
"base" (saved baseline) and "new" (most recent run) estimate files.

Exits with code 1 if any benchmark mean increases by more than the threshold
and the absolute increase meets the configured materiality floor.

Environment variables:
  CRITERION_DIR                 Path to Criterion output dir (default: target/criterion)
  PGT_BENCH_REGRESSION_THRESHOLD  Regression threshold as a fraction (default: 0.10 = 10%)

Usage:
  python3 scripts/criterion_regression_check.py [--threshold 0.10] [--dir target/criterion]
"""

import argparse
import json
import os
import sys
from pathlib import Path


def load_estimate(path: Path) -> float | None:
    """Return the mean estimate (ns) from a Criterion estimates.json file."""
    try:
        with open(path) as f:
            data = json.load(f)
        mean = data["mean"]
        estimate = mean.get("estimate", mean.get("point_estimate"))
        return float(estimate) if estimate is not None else None
    except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError, OSError):
        return None


def find_bench_pairs(criterion_dir: Path, baseline_name: str | None = None) -> list[tuple[str, float, float]]:
    """
    Return all (bench_label, base_ns, new_ns) triples where both base and
    new estimate files exist below criterion_dir.

    Criterion layout:
      <criterion_dir>/<group>/<bench>/base/estimates.json  ← saved baseline
      <criterion_dir>/<group>/<bench>/new/estimates.json   ← latest run
    """
    results: list[tuple[str, float, float]] = []

    for new_file in sorted(criterion_dir.rglob("new/estimates.json")):
        bench_dir = new_file.parent.parent
        base_file = bench_dir / baseline_name / "estimates.json" if baseline_name else bench_dir / "base" / "estimates.json"
        if baseline_name is None and not base_file.exists():
            # Criterion --save-baseline <name> stores under <name>/ not base/
            base_file = bench_dir / "main" / "estimates.json"
        if not base_file.exists():
            continue

        # Derive a human-readable label from the path components
        parts = new_file.parts
        # Find the criterion_dir prefix length and take the relevant suffix
        try:
            idx = parts.index(criterion_dir.parts[-1])
            label_parts = parts[idx + 1 : -2]  # strip criterion_dir + "new/estimates.json"
        except ValueError:
            label_parts = parts[:-2]
        label = "/".join(label_parts)

        base_ns = load_estimate(base_file)
        new_ns = load_estimate(new_file)
        if base_ns is None or new_ns is None:
            continue

        results.append((label, base_ns, new_ns))

    return results


def format_ns(ns: float) -> str:
    """Format nanoseconds into a human-readable string."""
    if ns < 1_000:
        return f"{ns:.1f} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.2f} µs"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    return f"{ns / 1_000_000_000:.3f} s"


def is_material_regression(base_ns: float, new_ns: float, threshold: float, minimum_delta_ns: float) -> bool:
    return new_ns - base_ns >= minimum_delta_ns and (new_ns - base_ns) / base_ns > threshold


def main() -> int:
    parser = argparse.ArgumentParser(description="Criterion regression gate")
    parser.add_argument(
        "--threshold",
        type=float,
        default=float(os.environ.get("PGT_BENCH_REGRESSION_THRESHOLD", "0.10")),
        help="Regression threshold as a fraction (default: 0.10 = 10%%)",
    )
    parser.add_argument(
        "--minimum-delta-ns",
        type=float,
        default=0.0,
        help="Ignore percentage regressions smaller than this absolute increase in ns",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path(os.environ.get("CRITERION_DIR", "target/criterion")),
        help="Path to Criterion output directory",
    )
    parser.add_argument("--baseline", help="Criterion baseline name to compare with the latest run")
    parser.add_argument("--require-pairs", action="store_true", help="Fail if there are no comparable benchmarks")
    parser.add_argument("--json-output", type=Path, help="Write machine-readable comparison results")
    args = parser.parse_args()

    criterion_dir: Path = args.dir
    threshold: float = args.threshold
    minimum_delta_ns: float = args.minimum_delta_ns
    if minimum_delta_ns < 0:
        parser.error("--minimum-delta-ns must be non-negative")

    if not criterion_dir.is_dir():
        print(f"::warning::Criterion output dir not found: {criterion_dir}")
        print("  No baseline to compare against — skipping regression check.")
        return 0

    pairs = find_bench_pairs(criterion_dir, args.baseline)
    if not pairs:
        if args.require_pairs:
            print("ERROR: no baseline/new pairs found — refusing an unmeasured pass")
            return 1
        print("::warning::No baseline/new pairs found — skipping regression check.")
        print(f"  (Searched: {criterion_dir})")
        return 0

    regressions: list[tuple[str, float, float, float]] = []
    subfloor_regressions: list[tuple[str, float, float, float]] = []
    improvements: list[tuple[str, float, float, float]] = []
    neutral: list[tuple[str, float, float, float]] = []

    for label, base_ns, new_ns in pairs:
        change = (new_ns - base_ns) / base_ns  # positive = slower
        if is_material_regression(base_ns, new_ns, threshold, minimum_delta_ns):
            regressions.append((label, base_ns, new_ns, change))
        elif change > threshold:
            subfloor_regressions.append((label, base_ns, new_ns, change))
        elif change < -threshold:
            improvements.append((label, base_ns, new_ns, change))
        else:
            neutral.append((label, base_ns, new_ns, change))

    comparisons = [
        {
            "label": label,
            "baseline_ns": baseline,
            "candidate_ns": candidate,
            "delta_ns": candidate - baseline,
            "regression_pct": (candidate / baseline - 1) * 100,
        }
        for label, baseline, candidate in pairs
    ]
    material_changes = [
        item["regression_pct"] for item in comparisons if item["delta_ns"] >= minimum_delta_ns
    ]
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(
                {
                    "kind": "criterion",
                    "baseline_version": args.baseline,
                    "compared_benchmarks": len(pairs),
                    "minimum_absolute_delta_ns": minimum_delta_ns,
                    "maximum_mean_regression_pct": max([0.0, *material_changes]),
                    "maximum_raw_mean_regression_pct": max(
                        [0.0, *(item["regression_pct"] for item in comparisons)]
                    ),
                    "subfloor_regressions": [
                        {
                            "label": label,
                            "baseline_ns": baseline,
                            "candidate_ns": candidate,
                            "delta_ns": candidate - baseline,
                            "regression_pct": change * 100,
                        }
                        for label, baseline, candidate, change in subfloor_regressions
                    ],
                    "benchmarks": comparisons,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    # ── Summary table ──────────────────────────────────────────────────────
    total = len(pairs)
    print(
        f"\nBenchmark regression check (threshold: {threshold * 100:.0f}%, "
        f"minimum absolute increase: {minimum_delta_ns:.1f} ns)"
    )
    print(f"Compared {total} benchmark(s) from {criterion_dir}\n")

    col_w = max((len(label) for label, *_ in pairs), default=20) + 2

    def row(label: str, base: float, new: float, pct: float) -> str:
        arrow = "▲" if pct > 0 else "▼" if pct < 0 else "="
        return (
            f"  {label:<{col_w}}  base={format_ns(base):>10}  "
            f"new={format_ns(new):>10}  {arrow} {pct * 100:+.1f}%"
        )

    if regressions:
        print("REGRESSIONS (slowdowns above threshold and materiality floor):")
        for item in regressions:
            print(row(*item))
        print()

    if subfloor_regressions:
        print("Below materiality floor (reported, not gated):")
        for item in subfloor_regressions:
            print(row(*item))
        print()

    if improvements:
        print("Improvements:")
        for item in improvements:
            print(row(*item))
        print()

    if neutral:
        print("Neutral (within threshold):")
        for item in neutral:
            print(row(*item))
        print()

    # ── GitHub Actions step summary ────────────────────────────────────────
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a") as f:
            f.write(f"## Benchmark Regression Check\n\n")
            f.write(f"Threshold: **{threshold * 100:.0f}%** &nbsp;|&nbsp; ")
            f.write(f"Minimum absolute increase: **{minimum_delta_ns:.1f} ns** &nbsp;|&nbsp; ")
            f.write(f"Compared: **{total}** benchmarks\n\n")

            if regressions:
                f.write("### ⚠️ Regressions\n\n")
                f.write("| Benchmark | Base | New | Change |\n")
                f.write("|-----------|------|-----|--------|\n")
                for label, base, new, pct in regressions:
                    f.write(
                        f"| `{label}` | {format_ns(base)} | {format_ns(new)} | "
                        f"🔴 **+{pct * 100:.1f}%** |\n"
                    )
                f.write("\n")
            if subfloor_regressions:
                f.write("### ℹ️ Below the materiality floor\n\n")
                f.write("| Benchmark | Base | New | Change |\n")
                f.write("|-----------|------|-----|--------|\n")
                for label, base, new, pct in subfloor_regressions:
                    f.write(
                        f"| `{label}` | {format_ns(base)} | {format_ns(new)} | "
                        f"+{pct * 100:.1f}% ({format_ns(new - base)}) |\n"
                    )
                f.write("\n")
            else:
                f.write("### ✅ No regressions detected\n\n")

            if improvements:
                f.write("### 🚀 Improvements\n\n")
                f.write("| Benchmark | Base | New | Change |\n")
                f.write("|-----------|------|-----|--------|\n")
                for label, base, new, pct in improvements:
                    f.write(
                        f"| `{label}` | {format_ns(base)} | {format_ns(new)} | "
                        f"🟢 {pct * 100:.1f}% |\n"
                    )
                f.write("\n")

    if regressions:
        print(
            f"ERROR: {len(regressions)} regression(s) detected "
            f"(threshold: {threshold * 100:.0f}%)"
        )
        return 1

    print(
        f"OK: all {total} benchmark(s) within threshold ({threshold * 100:.0f}%) "
        f"or below the {minimum_delta_ns:.1f} ns floor"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
