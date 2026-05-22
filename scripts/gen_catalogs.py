#!/usr/bin/env python3
"""gen_catalogs.py — Generate GUC and SQL API reference catalogs from source.

Parses src/config.rs for GUC definitions and src/**/*.rs for #[pg_extern]
SQL-callable functions, then writes:

  docs/GUC_CATALOG.md      — all GUC names, types, defaults, and doc comments
  docs/SQL_API_CATALOG.md  — all pgtrickle schema SQL-callable functions

Run:
  python3 scripts/gen_catalogs.py

CI drift check:
  python3 scripts/gen_catalogs.py --check
  (exits non-zero if committed catalogs differ from generated output)
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
DOCS_DIR = REPO_ROOT / "docs"

GUC_CATALOG_PATH = DOCS_DIR / "GUC_CATALOG.md"
SQL_CATALOG_PATH = DOCS_DIR / "SQL_API_CATALOG.md"

# ---------------------------------------------------------------------------
# GUC extraction
# ---------------------------------------------------------------------------

_GUC_NAME_RE = re.compile(r'c"(pg_trickle\.[^"]+)"')
_GUC_STATIC_RE = re.compile(
    r'pub static (PGS_\w+)\s*:\s*GucSetting<([^>]+(?:>[^>]*>)?)>'
)
_GUC_STATIC_REF_RE = re.compile(r'&(PGS_\w+)\s*,')
_GUC_DEFAULT_BOOL = re.compile(r"GucSetting::<bool>::new\((true|false)\)")
_GUC_DEFAULT_I32 = re.compile(r"GucSetting::<i32>::new\(([^)]+)\)")
_GUC_DEFAULT_F64 = re.compile(r"GucSetting::<f64>::new\(([^)]+)\)")
_GUC_DEFAULT_STR = re.compile(r'GucSetting::<Option<.*?>>::new\((?:Some\(c"([^"]+)"\)|None)\)')
_DOC_COMMENT_RE = re.compile(r"^\s*/// (.*)$")

# Mapping from Rust GucSetting type to PostgreSQL type name
_RUST_TO_PG_TYPE = {
    "bool": "bool",
    "i32": "int4",
    "f64": "float8",
}


def _rust_type_to_pg(type_str: str) -> str:
    """Convert a Rust GucSetting type string to a PostgreSQL type name."""
    if type_str in _RUST_TO_PG_TYPE:
        return _RUST_TO_PG_TYPE[type_str]
    if "Option" in type_str or "CString" in type_str:
        return "text"
    return type_str


def _build_static_to_guc_name_map(lines: list[str]) -> dict[str, str]:
    """Pre-scan the entire file to build a PGS_* → pg_trickle.* name map.

    Each GucRegistry::define_*_guc() block has the form:
        GucRegistry::define_bool_guc(
            c"pg_trickle.some_name",
            ...
            &PGS_SOME_NAME,
            ...
        );
    We extract both the name and the static reference from a sliding window.
    """
    mapping: dict[str, str] = {}
    n = len(lines)
    for i, line in enumerate(lines):
        if "GucRegistry::define_" not in line:
            continue
        # Scan forward up to 20 lines to find the guc name and the &PGS_* ref
        guc_name = None
        static_ref = None
        for j in range(i, min(i + 20, n)):
            if guc_name is None:
                nm = _GUC_NAME_RE.search(lines[j])
                if nm:
                    guc_name = nm.group(1)
            if static_ref is None:
                rm = _GUC_STATIC_REF_RE.search(lines[j])
                if rm:
                    static_ref = rm.group(1)
            if guc_name and static_ref:
                break
        if guc_name and static_ref:
            mapping[static_ref] = guc_name
    return mapping


def extract_gucs(config_rs: Path) -> list[dict]:
    """Extract GUC definitions from src/config.rs."""
    text = config_rs.read_text(encoding="utf-8")
    lines = text.splitlines()

    # Pass 1: build the PGS_* → pg_trickle.* name map from registration calls
    static_to_name = _build_static_to_guc_name_map(lines)

    gucs = []
    i = 0
    while i < len(lines):
        # Collect doc comment block
        doc_lines = []
        j = i
        while j < len(lines):
            m = _DOC_COMMENT_RE.match(lines[j])
            if m:
                doc_lines.append(m.group(1))
                j += 1
            else:
                break

        if not doc_lines:
            i += 1
            continue

        # Look for a GUC static declaration immediately following the doc block
        # (allow blank lines between doc and pub static)
        k = j
        while k < len(lines) and lines[k].strip() == "":
            k += 1

        static_line = lines[k] if k < len(lines) else ""
        sm = _GUC_STATIC_RE.match(static_line.strip())
        if sm:
            static_name = sm.group(1)
            rust_type = sm.group(2).strip()
            pg_type = _rust_type_to_pg(rust_type)

            # Resolve GUC name from pre-built map
            guc_name = static_to_name.get(static_name)

            # Skip entries with no registered GUC name (internal-only statics)
            if not guc_name:
                i = k + 1
                continue

            # Extract default value from the new(...) call
            default_val = "—"
            for scan in range(k, min(k + 5, len(lines))):
                seg = "\n".join(lines[scan : scan + 3])
                for pattern, fmt in [
                    (_GUC_DEFAULT_BOOL, lambda m: m.group(1)),
                    (_GUC_DEFAULT_I32, lambda m: m.group(1).replace("_", "")),
                    (_GUC_DEFAULT_F64, lambda m: m.group(1)),
                    (_GUC_DEFAULT_STR, lambda m: f'"{m.group(1)}"' if m.group(1) else "None"),
                ]:
                    dm = pattern.search(seg)
                    if dm:
                        default_val = fmt(dm)
                        break
                if default_val != "—":
                    break

            description = " ".join(doc_lines).strip()
            # Keep only the first sentence for brevity
            first_sentence = description.split(". ")[0].rstrip(".") + "."

            gucs.append(
                {
                    "static": static_name,
                    "name": guc_name,
                    "type": pg_type,
                    "default": default_val,
                    "description": first_sentence,
                }
            )
            i = k + 1
        else:
            i = j

    return gucs


# ---------------------------------------------------------------------------
# SQL API extraction
# ---------------------------------------------------------------------------

_PG_EXTERN_RE = re.compile(r'#\[(?:pgrx::)?pg_extern\s*\(([^)]*)\)\]')
_FN_SIG_RE = re.compile(
    r'(?:pub\s+)?(?:unsafe\s+)?fn\s+(\w+)\s*\(([^)]*(?:\([^)]*\)[^)]*)*)\)\s*(?:->\s*([^{;]+))?'
)


def extract_sql_functions(src_dir: Path) -> list[dict]:
    """Extract #[pg_extern] functions from all Rust source files."""
    functions = []

    for rs_file in sorted(src_dir.rglob("*.rs")):
        text = rs_file.read_text(encoding="utf-8")
        lines = text.splitlines()

        for idx, line in enumerate(lines):
            if not _PG_EXTERN_RE.search(line):
                continue

            attrs = _PG_EXTERN_RE.search(line).group(1)
            schema = "pgtrickle"
            m_schema = re.search(r'schema\s*=\s*"([^"]+)"', attrs)
            if m_schema:
                schema = m_schema.group(1)

            # Collect doc comments before the #[pg_extern]
            doc_lines = []
            back = idx - 1
            while back >= 0:
                m = _DOC_COMMENT_RE.match(lines[back])
                if m:
                    doc_lines.insert(0, m.group(1))
                    back -= 1
                elif lines[back].strip().startswith("#["):
                    back -= 1
                else:
                    break

            # Find the fn signature in the next few lines.
            # Join up to 10 lines so that multi-line argument lists (e.g.
            # when pgrx::default!() spans multiple lines) are handled by
            # the single-line regex.
            fn_name = None
            args_str = ""
            ret_str = ""
            for scan in range(idx + 1, min(idx + 10, len(lines))):
                # Try matching the current line alone first (fast path), then
                # with up to 9 subsequent lines joined (handles multi-line args).
                for window in range(1, min(10, len(lines) - scan + 1)):
                    joined = " ".join(lines[scan : scan + window])
                    fm = _FN_SIG_RE.search(joined)
                    if fm:
                        fn_name = fm.group(1)
                        args_str = fm.group(2).strip()
                        ret_str = (fm.group(3) or "").strip().rstrip("{").strip()
                        break
                if fn_name:
                    break

            if not fn_name:
                continue

            # Simplify argument list for display
            simple_args = re.sub(r"\s+", " ", args_str)

            description = " ".join(doc_lines).strip()
            first_sentence = description.split(". ")[0].rstrip(".") + "." if description else ""

            functions.append(
                {
                    "schema": schema,
                    "fn_name": fn_name,
                    "args": simple_args,
                    "returns": ret_str,
                    "file": str(rs_file.relative_to(REPO_ROOT)),
                    "description": first_sentence,
                }
            )

    return functions


# ---------------------------------------------------------------------------
# Catalog generation
# ---------------------------------------------------------------------------

GENERATED_HEADER = """\
<!-- AUTO-GENERATED — do not edit by hand.
     Run `python3 scripts/gen_catalogs.py` to regenerate.
     CI fails if this file is out of date with source code. -->
"""


def write_guc_catalog(gucs: list[dict], path: Path) -> str:
    """Return the generated GUC catalog content."""
    lines = [
        GENERATED_HEADER,
        "# GUC Reference — pg_trickle\n",
        f"**{len(gucs)} configuration parameters** extracted from `src/config.rs`.\n",
        "See [docs/CONFIGURATION.md](CONFIGURATION.md) for full descriptions and usage examples.\n",
        "",
        "| GUC name | Type | Default | Description |",
        "|----------|------|---------|-------------|",
    ]
    for g in sorted(gucs, key=lambda x: x["name"]):
        name = g["name"]
        typ = g["type"].replace("<", "\\<").replace(">", "\\>")
        default = g["default"]
        desc = g["description"].replace("|", "\\|")
        lines.append(f"| `{name}` | `{typ}` | `{default}` | {desc} |")

    return "\n".join(lines) + "\n"


def write_sql_catalog(funcs: list[dict], path: Path) -> str:
    """Return the generated SQL API catalog content."""
    lines = [
        GENERATED_HEADER,
        "# SQL API Reference — pg_trickle\n",
        f"**{len(funcs)} SQL-callable functions** discovered via `#[pg_extern]` in `src/`.\n",
        "See [docs/SQL_REFERENCE.md](SQL_REFERENCE.md) for full signatures and examples.\n",
        "",
        "| Function | Schema | Returns | Description |",
        "|----------|--------|---------|-------------|",
    ]
    for f in sorted(funcs, key=lambda x: (x["schema"], x["fn_name"])):
        fn = f["fn_name"]
        schema = f["schema"]
        ret = f["returns"].replace("|", "\\|")
        desc = f["description"].replace("|", "\\|")
        lines.append(f"| `{schema}.{fn}()` | `{schema}` | `{ret}` | {desc} |")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate pg_trickle API catalogs.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check mode: exit 1 if catalogs are out of date.",
    )
    args = parser.parse_args()

    config_rs = SRC_DIR / "config.rs"
    if not config_rs.exists():
        print(f"ERROR: {config_rs} not found", file=sys.stderr)
        return 1

    print("Extracting GUC definitions from src/config.rs …", flush=True)
    gucs = extract_gucs(config_rs)
    print(f"  Found {len(gucs)} GUC statics.", flush=True)

    print("Extracting SQL functions from src/ …", flush=True)
    funcs = extract_sql_functions(SRC_DIR)
    print(f"  Found {len(funcs)} #[pg_extern] functions.", flush=True)

    guc_content = write_guc_catalog(gucs, GUC_CATALOG_PATH)
    sql_content = write_sql_catalog(funcs, SQL_CATALOG_PATH)

    if args.check:
        drift = False
        for path, content in [(GUC_CATALOG_PATH, guc_content), (SQL_CATALOG_PATH, sql_content)]:
            if not path.exists():
                print(f"DRIFT: {path.relative_to(REPO_ROOT)} does not exist.", file=sys.stderr)
                drift = True
            elif path.read_text(encoding="utf-8") != content:
                print(
                    f"DRIFT: {path.relative_to(REPO_ROOT)} is out of date. "
                    "Run `python3 scripts/gen_catalogs.py` to regenerate.",
                    file=sys.stderr,
                )
                drift = True
            else:
                print(f"  OK: {path.relative_to(REPO_ROOT)}")
        return 1 if drift else 0

    DOCS_DIR.mkdir(exist_ok=True)
    GUC_CATALOG_PATH.write_text(guc_content, encoding="utf-8")
    SQL_CATALOG_PATH.write_text(sql_content, encoding="utf-8")
    print(f"Wrote {GUC_CATALOG_PATH.relative_to(REPO_ROOT)}")
    print(f"Wrote {SQL_CATALOG_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
