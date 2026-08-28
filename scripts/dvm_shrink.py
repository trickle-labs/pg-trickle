#!/usr/bin/env python3
"""Shrink a failing DVM scenario JSON to a minimal reproducer.

Companion to dvm_replay.py: reuses its scenario loader/validator and psql
runner instead of re-parsing the format. See roadmap COR-17.

The reducer tries cheap SQL/schema simplifications after structural shrinking;
each candidate is retained only when the original failure remains.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dvm_replay  # noqa: E402

_PK_UPDATE_RE = re.compile(
    r"^\s*UPDATE\b.*\bWHERE\s+\w+\s*=\s*\d+\s*(RETURNING\b.*)?;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_SQL_LITERAL_RE = re.compile(r"'[^']*'|\b\d+\b")
_CREATE_TABLE_RE = re.compile(
    r"(?P<prefix>CREATE\s+TABLE\s+[^()]+\()(?P<body>.*)(?P<suffix>\))",
    re.IGNORECASE | re.DOTALL,
)


def _split_sql_items(source: str) -> list[str]:
    """Split comma-separated SQL items while respecting quotes and nesting."""
    items: list[str] = []
    start = 0
    depth = 0
    quote = False
    for index, char in enumerate(source):
        if char == "'":
            quote = not quote
        elif not quote and char == "(":
            depth += 1
        elif not quote and char == ")":
            depth -= 1
        elif not quote and depth == 0 and char == ",":
            items.append(source[start:index].strip())
            start = index + 1
    items.append(source[start:].strip())
    return [item for item in items if item]


def _structural_reductions(scenario: dict) -> list[dict]:
    """Return cheap candidates for the remaining schema/query/settings ladder."""
    candidates: list[dict] = []

    # Constraints and types are local substitutions; replay decides whether the
    # failure signature survives the reduction.
    for index, statement in enumerate(scenario["schema"]["setup_sql"]):
        for pattern in (r"\s+PRIMARY\s+KEY", r"\s+NOT\s+NULL", r"\s+UNIQUE"):
            reduced = re.sub(pattern, "", statement, count=1, flags=re.IGNORECASE)
            if reduced != statement:
                candidate = copy.deepcopy(scenario)
                candidate["schema"]["setup_sql"][index] = reduced
                candidates.append(candidate)
        for source_type, target_type in (("BIGINT", "INT"), ("NUMERIC", "INT"), ("TEXT", "VARCHAR")):
            reduced = re.sub(rf"\b{source_type}\b", target_type, statement, count=1)
            if reduced != statement:
                candidate = copy.deepcopy(scenario)
                candidate["schema"]["setup_sql"][index] = reduced
                candidates.append(candidate)

        match = _CREATE_TABLE_RE.match(statement)
        if match:
            columns = _split_sql_items(match.group("body"))
            if len(columns) > 2:
                reduced = match.group("prefix") + ", ".join(columns[:-1]) + match.group("suffix")
                candidate = copy.deepcopy(scenario)
                candidate["schema"]["setup_sql"][index] = reduced
                candidates.append(candidate)

    defining_query = scenario["query"].get("defining_query", "")
    for pattern in (r"\s+SELECT\s+\*\s+FROM\s+", r"\s+AS\s+\w+"):
        reduced = re.sub(pattern, " ", defining_query, count=1, flags=re.IGNORECASE)
        if reduced != defining_query:
            candidate = copy.deepcopy(scenario)
            candidate["query"]["defining_query"] = reduced.strip()
            candidates.append(candidate)

    for pattern, replacement in (
        (r"\bLEFT\s+JOIN\b", "JOIN"),
        (r"\bFULL\s+JOIN\b", "JOIN"),
        (r"\bUNION\s+ALL\b", "UNION"),
        (r"\bDISTINCT\s+", ""),
    ):
        reduced = re.sub(pattern, replacement, defining_query, count=1, flags=re.IGNORECASE)
        if reduced != defining_query:
            candidate = copy.deepcopy(scenario)
            candidate["query"]["defining_query"] = reduced.strip()
            candidates.append(candidate)

    execution = scenario.get("execution", {})
    for key in list(execution):
        if key in {"schedule", "requested_refresh_mode"}:
            continue
        candidate = copy.deepcopy(scenario)
        del candidate["execution"][key]
        candidates.append(candidate)
    return candidates


def _split_top_level_tuples(values_src: str) -> list[str]:
    """Split "(a,b), (c,d)" into ["(a,b)", "(c,d)"] using paren-depth."""
    tuples: list[str] = []
    depth = 0
    start = None
    for i, ch in enumerate(values_src):
        if ch == "(":
            if depth == 0:
                start = i
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and start is not None:
                tuples.append(values_src[start : i + 1])
                start = None
    return tuples


def _parse_values(sql: str) -> tuple[str, list[str], str] | None:
    """Split an INSERT statement into (prefix, tuples, suffix) around VALUES."""
    match = re.search(r"\bVALUES\b", sql, re.IGNORECASE)
    if not match:
        return None
    prefix = sql[: match.end()]
    rest = sql[match.end() :]
    tuples = _split_top_level_tuples(rest)
    if not tuples:
        return None
    last_tuple_end = rest.rfind(tuples[-1]) + len(tuples[-1])
    suffix = rest[last_tuple_end:]
    return prefix, tuples, suffix


def _simplify_sql(sql: str) -> list[str]:
    """Generate safe, local reductions for literals in SQL statements."""
    candidates = []
    for match in _SQL_LITERAL_RE.finditer(sql):
        literal = match.group(0)
        replacement = "'x'" if literal.startswith("'") else "0"
        if literal != replacement:
            candidates.append(sql[: match.start()] + replacement + sql[match.end() :])
    return candidates


def shrink_scenario(scenario: dict, still_fails: Callable[[dict], bool]) -> dict:
    scenario = copy.deepcopy(scenario)
    changed = True
    while changed:
        changed = False

        # (a) remove whole cycles
        for i in range(len(scenario["cycles"]) - 1, -1, -1):
            if len(scenario["cycles"]) <= 1:
                break
            candidate = copy.deepcopy(scenario)
            del candidate["cycles"][i]
            if still_fails(candidate):
                scenario = candidate
                changed = True

        # (b) remove individual mutations within remaining cycles
        for ci, cycle in enumerate(scenario["cycles"]):
            for mi in range(len(cycle["mutations"]) - 1, -1, -1):
                if len(scenario["cycles"][ci]["mutations"]) <= 1:
                    break
                candidate = copy.deepcopy(scenario)
                del candidate["cycles"][ci]["mutations"][mi]
                if still_fails(candidate):
                    scenario = candidate
                    changed = True

        # (c) remove row-tuples from initial_data INSERT statements
        for si, stmt in enumerate(scenario["initial_data"]):
            parsed = _parse_values(stmt)
            if parsed is None:
                continue
            prefix, tuples, suffix = parsed
            for ti in range(len(tuples) - 1, -1, -1):
                current = _parse_values(scenario["initial_data"][si])
                if current is None:
                    break
                cur_prefix, cur_tuples, cur_suffix = current
                if len(cur_tuples) <= 1 or ti >= len(cur_tuples):
                    continue
                reduced = cur_tuples[:ti] + cur_tuples[ti + 1 :]
                new_stmt = cur_prefix + " " + ", ".join(reduced) + cur_suffix
                candidate = copy.deepcopy(scenario)
                candidate["initial_data"][si] = new_stmt
                if still_fails(candidate):
                    scenario = candidate
                    changed = True

        # (d) simplify mutation and setup literals (values, keys, aliases).
        for section in ("setup_sql", "initial_data"):
            statements = scenario["schema"][section] if section == "setup_sql" else scenario[section]
            for si, stmt in enumerate(statements):
                for reduced in _simplify_sql(stmt):
                    candidate = copy.deepcopy(scenario)
                    target = candidate["schema"][section] if section == "setup_sql" else candidate[section]
                    target[si] = reduced
                    if still_fails(candidate):
                        scenario = candidate
                        changed = True

        # (e) Remove schema constraints/columns, simplify types and query
        # operators, then drop execution knobs. The failure predicate keeps
        # only reductions preserving the original failure signature.
        for candidate in _structural_reductions(scenario):
            if still_fails(candidate):
                scenario = candidate
                changed = True
                break
        for ci, cycle in enumerate(scenario["cycles"]):
            for mi, mutation in enumerate(cycle["mutations"]):
                for reduced in _simplify_sql(mutation["sql"]):
                    candidate = copy.deepcopy(scenario)
                    candidate["cycles"][ci]["mutations"][mi]["sql"] = reduced
                    if still_fails(candidate):
                        scenario = candidate
                        changed = True

    return scenario


def _failure_class(error: BaseException) -> str:
    return str(error).split(":", 1)[0].split(";")[0].strip()


def replay_still_fails(
    scenario: dict, database: str | None, expected_class: str | None
) -> bool:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as handle:
        json.dump(scenario, handle)
        temp_path = Path(handle.name)
    try:
        dvm_replay.run(temp_path, database)
    except RuntimeError as error:
        if expected_class is None:
            return True
        return _failure_class(error) == expected_class
    except ValueError:
        return False
    else:
        return False
    finally:
        temp_path.unlink(missing_ok=True)


def static_injected_defect_present(scenario: dict) -> bool:
    for cycle in scenario["cycles"]:
        for mutation in cycle["mutations"]:
            if _PK_UPDATE_RE.match(mutation["sql"]) and mutation[
                "expected_affected_rows"
            ] != 1:
                return True
    return False


def _demo() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    scenario = dvm_replay.load(
        repo_root
        / "tests/corpus/dvm_negative_controls/negctrl_injected_939.json"
    )
    assert static_injected_defect_present(scenario) is True

    shrunk = shrink_scenario(scenario, static_injected_defect_present)

    # All padding cycles disappear, and since only the single offending
    # UPDATE carries the defect, its two innocent siblings in
    # "simultaneous-two-leaf-change" shrink away too -- the true minimal
    # reproducer is that one mutation on its own.
    assert len(shrunk["cycles"]) == 1, shrunk["cycles"]
    assert len(shrunk["cycles"][0]["mutations"]) == 1, shrunk["cycles"][0][
        "mutations"
    ]
    assert static_injected_defect_present(shrunk) is True
    print("dvm_shrink self-test OK")


def main() -> int:
    if len(sys.argv) == 2 and sys.argv[1] == "--selftest":
        _demo()
        return 0

    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--database")
    parser.add_argument("--expected-class")
    args = parser.parse_args()

    try:
        scenario = dvm_replay.load(args.scenario)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"dvm-shrink: {error}", file=sys.stderr)
        return 1

    expected_class = args.expected_class
    if not replay_still_fails(scenario, args.database, expected_class):
        print(
            "dvm-shrink: scenario does not currently fail; nothing to shrink",
            file=sys.stderr,
        )
        return 1

    if expected_class is None:
        try:
            dvm_replay.run(args.scenario, args.database)
        except RuntimeError as error:
            expected_class = _failure_class(error)

    before_cycles = len(scenario["cycles"])
    before_mutations = sum(len(c["mutations"]) for c in scenario["cycles"])
    before_initial = len(scenario["initial_data"])

    shrunk = shrink_scenario(
        scenario,
        lambda candidate: replay_still_fails(candidate, args.database, expected_class),
    )

    after_cycles = len(shrunk["cycles"])
    after_mutations = sum(len(c["mutations"]) for c in shrunk["cycles"])
    after_initial = len(shrunk["initial_data"])

    output = args.output or args.scenario
    output.write_text(json.dumps(shrunk, indent=2) + "\n", encoding="utf-8")

    print(
        f"dvm-shrink: cycles {before_cycles}->{after_cycles}, "
        f"mutations {before_mutations}->{after_mutations}, "
        f"initial_data {before_initial}->{after_initial}; wrote {output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
