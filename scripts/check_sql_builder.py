#!/usr/bin/env python3
"""Conservative data-flow check for variable-built SQL reaching SPI sinks."""
from pathlib import Path
import re
import sys

SQL = re.compile(r"\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|CALL|EXECUTE)\b", re.I)
SINK = re.compile(r"\b(?:Spi::(?:run|get_\w+|connect)|client\.(?:select|update|prepare)|query|prepare)\s*\(")
ASSIGN = re.compile(r"\blet\s+(?:mut\s+)?([A-Za-z_]\w*)\s*=\s*(.*)")
VAR_SINK = re.compile(r"(?:Spi::(?:run|get_\w+)|client\.(?:select|update|prepare))\s*\(\s*&?([A-Za-z_]\w*)")
MARKER = "nosemgrep:"


def reviewed(lines: list[str], index: int) -> bool:
    return any(
        MARKER in lines[j]
        for j in range(max(0, index - 3), min(len(lines), index + 4))
    )


def scan(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    builders: dict[str, int] = {}
    findings: list[str] = []
    for index, line in enumerate(lines):
        assignment = ASSIGN.search(line)
        if assignment:
            name, expr = assignment.groups()
            if SQL.search(expr) and (
                "format!" in expr or "write!" in expr or "push_str" in expr
                or ("{" in expr and "}" in expr) or "+" in expr
            ):
                builders[name] = index
        push = re.search(r"\b([A-Za-z_]\w*)\.push_str\s*\(\s*([\"'].*)", line)
        if push and SQL.search(push.group(2)):
            builders.setdefault(push.group(1), index)
        for name in list(builders):
            if re.search(rf"\b{name}\s*(?:\.push_str|\s*\+=|\.push\()", line):
                builders[name] = min(builders[name], index)
        sink = VAR_SINK.search(line)
        if sink and sink.group(1) in builders and not reviewed(lines, index):
            findings.append(f"{path}:{index + 1}: dynamic SQL variable '{sink.group(1)}' reaches a SPI sink")
        if SINK.search(line) and ("format!" in line or "format_args!" in line) and SQL.search(line):
            if not reviewed(lines, index):
                findings.append(f"{path}:{index + 1}: interpolated SQL is passed directly to a SPI sink")
    return findings


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "src"
    findings = [finding for path in root.rglob("*.rs") if "/tests/" not in str(path) for finding in scan(path)]
    if findings:
        print("SEC-003: SQL builder audit FAILED")
        print("\n".join(findings))
        print("Use bind parameters or a narrow nosemgrep marker with a reason for identifier SQL.")
        return 1
    print("SEC-003: SQL builder audit PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
