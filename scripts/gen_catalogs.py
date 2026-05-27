#!/usr/bin/env python3
"""gen_catalogs.py — Generate GUC and SQL API reference catalogs from source.

Parses src/config.rs for GUC definitions and src/**/*.rs for #[pg_extern]
SQL-callable functions, then writes:

  docs/GUC_CATALOG.md      — all GUC names, types, defaults, and doc comments
  docs/SQL_API_CATALOG.md  — all pgtrickle schema SQL-callable functions

Source of truth (in priority order):
  1. pgrx-generated SQL file if available (target/*/release/pg_trickle.sql or
     target/release/pg_trickle.sql).  Produced by `cargo pgrx schema`.
  2. Regex extraction from Rust source (improved multiline return-type handling).

Run:
  python3 scripts/gen_catalogs.py

CI drift check:
  python3 scripts/gen_catalogs.py --check
  (exits non-zero if committed catalogs differ from generated output, or if
  any return type fails the quality gate)
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
# SQL API extraction — pgrx SQL output parser (DOC-001/CODE-001)
# ---------------------------------------------------------------------------

def _find_pgrx_sql_output() -> Path | None:
    """Locate the pgrx-generated SQL file if a recent build exists."""
    candidates = [
        REPO_ROOT / "target" / "release" / "pg_trickle.sql",
        REPO_ROOT / "target" / "pg18" / "release" / "pg_trickle.sql",
        REPO_ROOT / "target" / "pg17" / "release" / "pg_trickle.sql",
    ]
    # Also check pgrx package output layout
    for pg in ("pg18", "pg17"):
        candidates.append(
            REPO_ROOT / "target" / f"pg_trickle-pg{pg[-2:]}" / f"pg_trickle--{pg}.sql"
        )
    for p in candidates:
        if p.exists():
            return p
    return None


def _angle_bracket_depth(s: str) -> int:
    """Return net depth of < > in string (positive = unclosed open brackets)."""
    return s.count("<") - s.count(">")


def _normalize_return_type(raw: str) -> str:
    """Normalise a Rust return type string for display in the catalog.

    Rules (applied in order):
    1. Strip leading/trailing whitespace and trailing ``{``.
    2. Collapse internal whitespace.
    3. Result<TableIterator<...>, ...>  → SetOf row (failable)
    4. TableIterator<...>               → SetOf row
    5. Result<(), ...>                  → void
    6. Result<T, ...>                   → SQL type of T
    7. pgrx::JsonB                      → jsonb
    8. crate::error::PgTrickleError     → PgTrickleError
    9. Option<T>                        → T (nullable)
    10. Rust primitive types             → SQL equivalents
    11. &'static str / &str             → text
    12. InitDecision (internal struct)  → (internal)
    """
    s = raw.rstrip("{").strip()
    s = re.sub(r"\s+", " ", s)
    # Strip lifetime annotations like 'static, from inside brackets
    s = re.sub(r"'\w+,\s*", "", s)

    if not s:
        return s

    # Result<TableIterator<...>, ...> → "SetOf row (failable)"
    if s.startswith("Result<") and "TableIterator<" in s:
        return "SetOf row (failable)"

    # TableIterator<...> → "SetOf row"
    if s.startswith("TableIterator"):
        return "SetOf row"

    # Result<(), ...> → "void"
    if re.match(r"Result\s*<\s*\(\s*\)\s*,", s):
        return "void"

    # Result<String, ...> → "text"
    if re.match(r"Result\s*<\s*String\s*,", s):
        return "text"

    # Result<i64, ...> → "bigint"
    if re.match(r"Result\s*<\s*i64\s*,", s):
        return "bigint"

    # Result<i32, ...> → "integer"
    if re.match(r"Result\s*<\s*i32\s*,", s):
        return "integer"

    # Result<bool, ...> → "boolean"
    if re.match(r"Result\s*<\s*bool\s*,", s):
        return "boolean"

    # pgrx::JsonB → "jsonb"
    s = re.sub(r"pgrx::JsonB\b", "jsonb", s)

    # Normalize full module paths for PgTrickleError
    s = re.sub(r"crate::error::PgTrickleError", "PgTrickleError", s)
    s = re.sub(r"crate::\w+::PgTrickleError", "PgTrickleError", s)

    # Rust primitive types → SQL equivalents (API-004, v0.75.0)
    # Apply BEFORE Option<T> unwrapping so Option<String> → text (nullable)
    s = re.sub(r"\bString\b", "text", s)
    s = re.sub(r"&\s*'?\w*\s*str\b", "text", s)
    s = re.sub(r"\bi64\b", "bigint", s)
    s = re.sub(r"\bi32\b", "integer", s)
    s = re.sub(r"\bi16\b", "smallint", s)
    s = re.sub(r"\bu64\b", "bigint", s)
    s = re.sub(r"\bu32\b", "bigint", s)
    s = re.sub(r"\bf64\b", "double precision", s)
    s = re.sub(r"\bf32\b", "real", s)
    s = re.sub(r"\bbool\b", "boolean", s)

    # Internal Rust structs not visible as SQL types
    s = re.sub(r"\bInitDecision\b", "(internal)", s)

    # Option<T> → T (nullable) — apply after primitive type conversions
    s = re.sub(r"Option\s*<([A-Za-z0-9_: ]+?)>", r"\1 (nullable)", s)

    return s


def extract_sql_functions_from_pgrx_sql(sql_path: Path) -> list[dict]:
    """Parse pgrx-generated SQL to extract CREATE FUNCTION statements.

    Uses a simple context-free parser that tracks parenthesis depth so that
    multi-line argument lists and return types with nested generics are fully
    captured.
    """
    text = sql_path.read_text(encoding="utf-8")
    functions = []

    # Split on CREATE FUNCTION / CREATE OR REPLACE FUNCTION boundaries
    # The pgrx SQL uses LANGUAGE c with dollar-quoted bodies or no body.
    pattern = re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+'
        r'"?(\w+)"?\."?(\w+)"?\s*\(([^;]*?)\)\s+'
        r'RETURNS\s+([^;]+?)\s+'
        r'LANGUAGE\s+\w+',
        re.IGNORECASE | re.DOTALL,
    )
    for m in pattern.finditer(text):
        schema = m.group(1)
        fn_name = m.group(2)
        # args are in PostgreSQL syntax, simplify for display
        args_str = re.sub(r"\s+", " ", m.group(3).strip())
        ret_raw = re.sub(r"\s+", " ", m.group(4).strip()).rstrip(";{").strip()

        # Normalise TABLE(...) → SetOf row
        if ret_raw.upper().startswith("TABLE"):
            ret_raw = "SetOf row"

        functions.append({
            "schema": schema,
            "fn_name": fn_name,
            "args": args_str,
            "returns": ret_raw,
            "file": str(sql_path.relative_to(REPO_ROOT)),
            "description": "",
        })

    return functions


# ---------------------------------------------------------------------------
# SQL API extraction — improved Rust source regex parser
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

            # Prefer the SQL-visible name from name = "..." over the Rust fn name
            sql_name_override: str | None = None
            m_name = re.search(r'\bname\s*=\s*"([^"]+)"', attrs)
            if m_name:
                sql_name_override = m_name.group(1)

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
            # Join up to 25 lines to capture complex multiline return types
            # such as Result<TableIterator<'static, (name!(...), ...)>, Error>.
            # DOC-001/CODE-001: keep extending the window while the return type
            # has unbalanced angle brackets (e.g. ``Result<`` alone).
            fn_name = None
            args_str = ""
            ret_str = ""
            for scan in range(idx + 1, min(idx + 5, len(lines))):
                best_fn: str | None = None
                best_args = ""
                best_ret = ""
                for window in range(1, min(25, len(lines) - scan + 1)):
                    joined = " ".join(lines[scan : scan + window])
                    fm = _FN_SIG_RE.search(joined)
                    if fm:
                        candidate_ret = (fm.group(3) or "").strip()
                        if best_fn is None:
                            best_fn = fm.group(1)
                            best_args = fm.group(2).strip()
                            best_ret = candidate_ret
                        else:
                            # Prefer the longer / more complete return type
                            if len(candidate_ret) > len(best_ret):
                                best_ret = candidate_ret
                        # Stop extending when angle brackets are balanced
                        if _angle_bracket_depth(candidate_ret) == 0:
                            best_fn = fm.group(1)
                            best_args = fm.group(2).strip()
                            best_ret = candidate_ret
                            break
                if best_fn:
                    fn_name = best_fn
                    args_str = best_args
                    ret_str = best_ret
                    break

            if not fn_name:
                continue

            # Use SQL name override (from name = "...") if present
            if sql_name_override:
                fn_name = sql_name_override

            # Normalise return type using the shared normaliser.
            ret_clean = _normalize_return_type(ret_str)

            # Simplify argument list for display
            simple_args = re.sub(r"\s+", " ", args_str)

            description = " ".join(doc_lines).strip()
            first_sentence = description.split(". ")[0].rstrip(".") + "." if description else ""

            functions.append(
                {
                    "schema": schema,
                    "fn_name": fn_name,
                    "args": simple_args,
                    "returns": ret_clean,
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


def validate_catalog(funcs: list[dict]) -> list[str]:
    """DOC-001/CODE-001: Quality gate — return list of error messages.

    Fails on:
    - Return type that ends with ``<`` (truncated nested generic).
    - Return type with unbalanced ``<>`` brackets.
    - Non-empty return type containing a bare ``<`` at position > 0 that
      suggests mid-capture truncation (e.g. ``Result<``).
    """
    errors: list[str] = []
    for f in funcs:
        ret = f.get("returns", "") or ""
        fn_id = f"{f['schema']}.{f['fn_name']}()"

        if ret.endswith("<"):
            errors.append(f"{fn_id}: return type truncated (ends with '<'): {ret!r}")
            continue

        depth = _angle_bracket_depth(ret)
        if depth != 0:
            errors.append(
                f"{fn_id}: return type has unbalanced angle brackets "
                f"(depth={depth}): {ret!r}"
            )

    return errors


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
        help="Check mode: exit 1 if catalogs are out of date or fail quality gate.",
    )
    args = parser.parse_args()

    config_rs = SRC_DIR / "config.rs"
    if not config_rs.exists():
        print(f"ERROR: {config_rs} not found", file=sys.stderr)
        return 1

    print("Extracting GUC definitions from src/config.rs …", flush=True)
    gucs = extract_gucs(config_rs)
    print(f"  Found {len(gucs)} GUC statics.", flush=True)

    # DOC-001/CODE-001: prefer pgrx SQL output if available; fall back to regex.
    pgrx_sql = _find_pgrx_sql_output()
    if pgrx_sql:
        print(f"Extracting SQL functions from pgrx SQL output: {pgrx_sql.relative_to(REPO_ROOT)} …", flush=True)
        funcs = extract_sql_functions_from_pgrx_sql(pgrx_sql)
        print(f"  Found {len(funcs)} functions.", flush=True)
    else:
        print("Extracting SQL functions from src/ (pgrx SQL output not found) …", flush=True)
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

        # Quality gate: run against the COMMITTED catalog (reproducibility
        # check above already verified it matches generated output).
        qual_errors = validate_catalog(funcs)
        if qual_errors:
            print("\nQUALITY GATE FAILURES:", file=sys.stderr)
            for e in qual_errors:
                print(f"  {e}", file=sys.stderr)
            drift = True
        else:
            print(f"  Quality gate: {len(funcs)} function(s) — all return types valid.")

        return 1 if drift else 0

    DOCS_DIR.mkdir(exist_ok=True)
    GUC_CATALOG_PATH.write_text(guc_content, encoding="utf-8")
    SQL_CATALOG_PATH.write_text(sql_content, encoding="utf-8")
    print(f"Wrote {GUC_CATALOG_PATH.relative_to(REPO_ROOT)}")
    print(f"Wrote {SQL_CATALOG_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
