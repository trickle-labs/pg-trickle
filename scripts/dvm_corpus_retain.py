#!/usr/bin/env python3
"""Retain only DVM regression scenarios that add semantic coverage.

Greedy set-cover over each scenario's `features` dict. See roadmap COR-17:
"Retain passing cases only when they add semantic coverage. Periodically
remove redundant cases with a greedy set-cover pass."
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CORPUS_DIR = REPO_ROOT / "tests/corpus/dvm_regressions"
HISTORICAL_ANCHORS = {"cor938_physical_width.json", "cor939_two_leaf_snapshot.json"}


def coverage_keys(scenario: dict) -> set[str]:
    features = scenario.get("features", {})
    keys: set[str] = set()
    keys |= {"aggregate:" + a for a in features.get("aggregates", [])}
    keys |= {"join:" + j for j in features.get("joins", [])}
    if features.get("simultaneous_source_changes"):
        keys.add("simultaneous_source_changes")
    if features.get("nullable_groups"):
        keys.add("nullable_groups")
    if features.get("duplicate_rows"):
        keys.add("duplicate_rows")
    keys |= {
        "changed_leaf_bucket:" + str(b)
        for b in features.get("changed_leaf_buckets", [])
    }
    keys |= {
        "mutation_intent:" + m for m in features.get("mutation_intents", [])
    }
    return keys


def load_corpus() -> dict[str, set[str]]:
    coverage: dict[str, set[str]] = {}
    for path in sorted(CORPUS_DIR.glob("*.json")):
        with path.open(encoding="utf-8") as handle:
            scenario = json.load(handle)
        coverage[path.name] = coverage_keys(scenario)
    return coverage


def greedy_set_cover(coverage: dict[str, set[str]]) -> tuple[list[str], list[str]]:
    remaining = dict(coverage)
    covered: set[str] = set()
    selected: list[str] = []
    while True:
        best_name = None
        best_new: set[str] = set()
        for name in sorted(remaining):
            new_keys = remaining[name] - covered
            if len(new_keys) > len(best_new):
                best_name = name
                best_new = new_keys
        if best_name is None or not best_new:
            break
        selected.append(best_name)
        covered |= best_new
        del remaining[best_name]
    redundant = sorted(set(coverage) - set(selected))
    return selected, redundant


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    coverage = load_corpus()
    selected, redundant = greedy_set_cover(coverage)

    print(f"selected {len(selected)} of {len(coverage)} scenario(s):")
    for name in selected:
        print(f"  keep {name}")

    if not redundant:
        print("no redundant scenarios found")
        return 0

    for name in redundant:
        keys = coverage[name]
        # A redundant scenario's keys are a subset of the union of already
        # selected keys; report which already-selected files cover them.
        covering = _find_covering(keys, {n: coverage[n] for n in selected})
        print(f"  redundant {name} (covered by: {', '.join(covering) or 'none'})")
        if args.apply:
            if name in HISTORICAL_ANCHORS:
                print("    kept (historical anchor)")
            else:
                (CORPUS_DIR / name).unlink()
                print(f"    deleted {name}")

    return 1


def _find_covering(keys: set[str], selected_coverage: dict[str, set[str]]) -> list[str]:
    remaining = set(keys)
    covering: list[str] = []
    for name in sorted(selected_coverage):
        if not remaining:
            break
        overlap = selected_coverage[name] & remaining
        if overlap:
            covering.append(name)
            remaining -= overlap
    return covering


if __name__ == "__main__":
    raise SystemExit(main())
