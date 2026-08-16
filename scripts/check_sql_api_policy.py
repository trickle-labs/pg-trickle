#!/usr/bin/env python3
"""Validate and generate the explicit SQL API policy for pg_trickle.

The checked-in policy is keyed by full SQL function identity:
    schema.name(identity_arguments)

The tool can inspect either packaged SQL (default) or a live catalog and then:
  * fail on unclassified functions,
  * fail on duplicate signatures in the inspected source,
  * fail on duplicate keys in the policy JSON,
  * explain missing signatures with exact overload identities, and
  * emit deny-first ACL SQL from the explicit policy.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import unittest
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_POLICY_PATH = REPO_ROOT / "scripts" / "sql_api_policy.json"
CARGO_TOML = REPO_ROOT / "Cargo.toml"

API_CLASSES = {
    "public_read",
    "owner_lifecycle",
    "admin_global",
    "arbitrary_sql",
    "trigger_entry",
    "internal",
}

_TRIGGER_ENTRY_NAMES = {
    "_on_ddl_end",
    "_on_sql_drop",
}

_INTERNAL_NAMES = {
    "_signal_launcher_rescan",
    "handle_vp_promoted",
    "pgt_ivm_apply_delta",
    "pgt_ivm_apply_delta_enr",
    "pgt_ivm_handle_truncate",
}

_ARBITRARY_SQL_NAMES = {
    "write_and_refresh",
}

_ADMIN_GLOBAL_NAMES = {
    "advance_watermark",
    "clear_caches",
    "convert_buffers_to_unlogged",
    "create_refresh_group",
    "create_watermark_group",
    "drain",
    "drop_refresh_group",
    "drop_watermark_group",
    "gate_source",
    "migrate",
    "pause_all",
    "pause_scheduler",
    "rebuild_cdc_triggers",
    "restore_stream_tables",
    "resume_all",
    "resume_scheduler",
    "setup_self_monitoring",
    "teardown_self_monitoring",
    "ungate_source",
}

_OWNER_LIFECYCLE_PREFIXES = (
    "alter_",
    "attach_",
    "bulk_",
    "canary_",
    "create_",
    "detach_",
    "drop_",
    "pause_",
    "refresh_",
    "repair_",
    "restore_",
    "resume_",
    "set_stream_table_",
    "snapshot_",
)

_OWNER_LIFECYCLE_NAMES = {
    "embedding_stream_table",
    "exec_stream_ddl",
    "refresh_if_stale",
    "stream_table_to_publication",
    "subscribe",
    "subscribe_distance",
    "unsubscribe",
    "unsubscribe_distance",
}

_TYPE_ALIASES = {
    "bool": "boolean",
    "float8": "double precision",
    "int": "integer",
    "int2": "smallint",
    "int4": "integer",
    "int8": "bigint",
    "timestamptz": "timestamp with time zone",
    "timetz": "time with time zone",
    "varchar": "character varying",
}

_CREATE_FUNCTION_RE = re.compile(
    r"""CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+
        (?P<schema>"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)\.
        (?P<name>"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)\s*\(
    """,
    re.IGNORECASE | re.VERBOSE,
)


class ApiPolicyError(ValueError):
    """Raised when the policy file or source signature set is invalid."""


@dataclass(frozen=True, order=True)
class FunctionIdentity:
    schema: str
    name: str
    identity_arguments: str

    @property
    def key(self) -> str:
        return f"{self.schema}.{self.name}({self.identity_arguments})"

    @property
    def sql_signature(self) -> str:
        return f"{quote_ident(self.schema)}.{quote_ident(self.name)}({self.identity_arguments})"


@dataclass(frozen=True)
class PolicyEntry:
    identity: FunctionIdentity
    api_class: str


@dataclass(frozen=True)
class SuggestedClassification:
    api_class: str
    rationale: str


@dataclass(frozen=True)
class ValidationReport:
    duplicate_source_signatures: tuple[str, ...]
    missing_policy_signatures: tuple[str, ...]
    extra_policy_signatures: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not (
            self.duplicate_source_signatures
            or self.missing_policy_signatures
            or self.extra_policy_signatures
        )


def quote_ident(identifier: str) -> str:
    if re.fullmatch(r"[a-z_][a-z0-9_]*", identifier):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def unquote_identifier(token: str) -> str:
    token = token.strip()
    if token.startswith('"') and token.endswith('"'):
        return token[1:-1].replace('""', '"')
    return token.lower()


def collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def default_archive_sql_path() -> Path:
    cargo_text = CARGO_TOML.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', cargo_text, re.MULTILINE)
    if not match:
        raise ApiPolicyError(f"could not read version from {CARGO_TOML}")
    version = match.group(1)
    archive_path = REPO_ROOT / "sql" / "archive" / f"pg_trickle--{version}.sql"
    if archive_path.is_file():
        return archive_path

    if DEFAULT_POLICY_PATH.is_file():
        try:
            document = json.loads(DEFAULT_POLICY_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            document = None
        if isinstance(document, dict):
            generated_from = document.get("generated_from")
            if isinstance(generated_from, str):
                fallback_path = (REPO_ROOT / generated_from).resolve()
                if fallback_path.is_file():
                    return fallback_path

    raise ApiPolicyError(
        f"packaged SQL file not found for Cargo version {version}: {archive_path}. "
        "Pass --source-sql explicitly or install/package the matching archive."
    )


def _scan_default_separator(param: str) -> int | None:
    i = 0
    depth = 0
    in_single = False
    in_double = False
    block_comment_depth = 0
    in_line_comment = False

    while i < len(param):
        if in_line_comment:
            if param[i] == "\n":
                in_line_comment = False
            i += 1
            continue

        if block_comment_depth:
            if param.startswith("/*", i):
                block_comment_depth += 1
                i += 2
                continue
            if param.startswith("*/", i):
                block_comment_depth -= 1
                i += 2
                continue
            i += 1
            continue

        if in_single:
            if param.startswith("''", i):
                i += 2
                continue
            if param[i] == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            if param.startswith('""', i):
                i += 2
                continue
            if param[i] == '"':
                in_double = False
            i += 1
            continue

        if param.startswith("--", i):
            in_line_comment = True
            i += 2
            continue
        if param.startswith("/*", i):
            block_comment_depth = 1
            i += 2
            continue
        if param[i] == "'":
            in_single = True
            i += 1
            continue
        if param[i] == '"':
            in_double = True
            i += 1
            continue
        if param[i] == "(":
            depth += 1
            i += 1
            continue
        if param[i] == ")":
            if depth > 0:
                depth -= 1
            i += 1
            continue
        if depth == 0 and param[i] == "=":
            return i
        if depth == 0 and param[i].isalpha():
            maybe_default = param[i : i + 7]
            if maybe_default.upper() == "DEFAULT":
                before_ok = i == 0 or not (param[i - 1].isalnum() or param[i - 1] == "_")
                after_index = i + 7
                after_ok = after_index >= len(param) or not (
                    param[after_index].isalnum() or param[after_index] == "_"
                )
                if before_ok and after_ok:
                    return i
        i += 1

    return None


def strip_sql_comments(text: str) -> str:
    out: list[str] = []
    i = 0
    in_single = False
    in_double = False
    block_comment_depth = 0
    in_line_comment = False

    while i < len(text):
        if in_line_comment:
            if text[i] == "\n":
                in_line_comment = False
                out.append("\n")
            i += 1
            continue

        if block_comment_depth:
            if text.startswith("/*", i):
                block_comment_depth += 1
                i += 2
                continue
            if text.startswith("*/", i):
                block_comment_depth -= 1
                i += 2
                continue
            i += 1
            continue

        if in_single:
            if text.startswith("''", i):
                out.append("''")
                i += 2
                continue
            out.append(text[i])
            if text[i] == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            if text.startswith('""', i):
                out.append('""')
                i += 2
                continue
            out.append(text[i])
            if text[i] == '"':
                in_double = False
            i += 1
            continue

        if text.startswith("--", i):
            in_line_comment = True
            i += 2
            continue
        if text.startswith("/*", i):
            block_comment_depth = 1
            i += 2
            continue
        if text[i] == "'":
            in_single = True
            out.append(text[i])
            i += 1
            continue
        if text[i] == '"':
            in_double = True
            out.append(text[i])
            i += 1
            continue

        out.append(text[i])
        i += 1

    return "".join(out)


def split_top_level_commas(text: str) -> list[str]:
    parts: list[str] = []
    start = 0
    i = 0
    depth = 0
    in_single = False
    in_double = False
    block_comment_depth = 0
    in_line_comment = False

    while i < len(text):
        if in_line_comment:
            if text[i] == "\n":
                in_line_comment = False
            i += 1
            continue

        if block_comment_depth:
            if text.startswith("/*", i):
                block_comment_depth += 1
                i += 2
                continue
            if text.startswith("*/", i):
                block_comment_depth -= 1
                i += 2
                continue
            i += 1
            continue

        if in_single:
            if text.startswith("''", i):
                i += 2
                continue
            if text[i] == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            if text.startswith('""', i):
                i += 2
                continue
            if text[i] == '"':
                in_double = False
            i += 1
            continue

        if text.startswith("--", i):
            in_line_comment = True
            i += 2
            continue
        if text.startswith("/*", i):
            block_comment_depth = 1
            i += 2
            continue
        if text[i] == "'":
            in_single = True
            i += 1
            continue
        if text[i] == '"':
            in_double = True
            i += 1
            continue
        if text[i] == "(":
            depth += 1
            i += 1
            continue
        if text[i] == ")":
            if depth > 0:
                depth -= 1
            i += 1
            continue
        if text[i] == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
        i += 1

    parts.append(text[start:])
    return parts


def find_matching_paren(text: str, open_index: int) -> int:
    i = open_index + 1
    depth = 1
    in_single = False
    in_double = False
    block_comment_depth = 0
    in_line_comment = False

    while i < len(text):
        if in_line_comment:
            if text[i] == "\n":
                in_line_comment = False
            i += 1
            continue

        if block_comment_depth:
            if text.startswith("/*", i):
                block_comment_depth += 1
                i += 2
                continue
            if text.startswith("*/", i):
                block_comment_depth -= 1
                i += 2
                continue
            i += 1
            continue

        if in_single:
            if text.startswith("''", i):
                i += 2
                continue
            if text[i] == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            if text.startswith('""', i):
                i += 2
                continue
            if text[i] == '"':
                in_double = False
            i += 1
            continue

        if text.startswith("--", i):
            in_line_comment = True
            i += 2
            continue
        if text.startswith("/*", i):
            block_comment_depth = 1
            i += 2
            continue
        if text[i] == "'":
            in_single = True
            i += 1
            continue
        if text[i] == '"':
            in_double = True
            i += 1
            continue
        if text[i] == "(":
            depth += 1
            i += 1
            continue
        if text[i] == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1

    raise ApiPolicyError("unterminated function argument list in packaged SQL")


def split_first_token(text: str) -> tuple[str, str]:
    text = text.lstrip()
    if not text:
        raise ApiPolicyError("expected SQL token, got empty text")
    if text[0] == '"':
        i = 1
        while i < len(text):
            if text.startswith('""', i):
                i += 2
                continue
            if text[i] == '"':
                return text[: i + 1], text[i + 1 :]
            i += 1
        raise ApiPolicyError(f"unterminated quoted identifier in parameter: {text!r}")

    match = re.match(r"\S+", text)
    if not match:
        raise ApiPolicyError(f"could not parse token from parameter: {text!r}")
    return match.group(0), text[match.end() :]


def normalize_type_name(type_name: str) -> str:
    normalized = collapse_whitespace(type_name)
    normalized = normalized.replace(" [", "[").replace(" ]", "]")
    lowered = normalized.lower()

    variadic_prefix = ""
    if lowered.startswith("variadic "):
        variadic_prefix = "variadic "
        lowered = lowered[len("variadic ") :].strip()

    array_suffix = ""
    while lowered.endswith("[]"):
        array_suffix += "[]"
        lowered = lowered[:-2].strip()

    lowered = _TYPE_ALIASES.get(lowered, lowered)
    return f"{variadic_prefix}{lowered}{array_suffix}"


def parse_identity_arguments(raw_args: str) -> str:
    types: list[str] = []
    for raw_param in split_top_level_commas(raw_args):
        param = collapse_whitespace(strip_sql_comments(raw_param))
        if not param:
            continue

        default_index = _scan_default_separator(param)
        if default_index is not None:
            param = param[:default_index].rstrip()

        token, remainder = split_first_token(param)
        mode = token.upper()
        include_mode = False

        if mode in {"IN", "OUT", "INOUT", "VARIADIC", "TABLE"}:
            if mode in {"OUT", "TABLE"}:
                continue
            include_mode = mode == "VARIADIC"
            _, remainder = split_first_token(remainder)
        remainder = remainder.strip()
        if not remainder:
            raise ApiPolicyError(f"could not parse type from parameter: {raw_param!r}")

        type_name = normalize_type_name(
            f"variadic {remainder}" if include_mode else remainder
        )
        types.append(type_name)

    return ", ".join(types)


def parse_packaged_sql_functions(sql_text: str) -> list[FunctionIdentity]:
    identities: list[FunctionIdentity] = []
    for match in _CREATE_FUNCTION_RE.finditer(sql_text):
        schema = unquote_identifier(match.group("schema"))
        name = unquote_identifier(match.group("name"))
        open_paren = sql_text.find("(", match.start())
        if open_paren < 0:
            raise ApiPolicyError(f"could not locate argument list for {schema}.{name}")
        close_paren = find_matching_paren(sql_text, open_paren)
        raw_args = sql_text[open_paren + 1 : close_paren]
        identity_arguments = parse_identity_arguments(raw_args)
        identities.append(
            FunctionIdentity(
                schema=schema,
                name=name,
                identity_arguments=identity_arguments,
            )
        )
    return identities


def load_packaged_sql(path: Path) -> list[FunctionIdentity]:
    if not path.is_file():
        raise ApiPolicyError(f"packaged SQL file not found: {path}")
    return parse_packaged_sql_functions(path.read_text(encoding="utf-8"))


def load_live_catalog(dsn: str, extension_name: str, psql_bin: str) -> list[FunctionIdentity]:
    query = f"""
SELECT
    n.nspname::text,
    p.proname::text,
    pg_catalog.pg_get_function_identity_arguments(p.oid)
FROM pg_catalog.pg_extension e
JOIN pg_catalog.pg_depend d
  ON d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
 AND d.refobjid = e.oid
 AND d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
 AND d.deptype = 'e'
JOIN pg_catalog.pg_proc p
  ON p.oid = d.objid
JOIN pg_catalog.pg_namespace n
  ON n.oid = p.pronamespace
WHERE e.extname = {sql_literal(extension_name)}
ORDER BY 1, 2, 3
""".strip()
    try:
        completed = subprocess.run(
            [psql_bin, dsn, "-X", "-A", "-F", "\t", "-t", "-c", query],
            check=True,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError as exc:
        raise ApiPolicyError(
            f"psql binary not found: {psql_bin}. Install PostgreSQL client tools or use --source-sql."
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() or exc.stdout.strip() or str(exc)
        raise ApiPolicyError(f"live catalog query failed: {stderr}") from exc

    identities: list[FunctionIdentity] = []
    for raw_line in completed.stdout.splitlines():
        if not raw_line.strip():
            continue
        parts = raw_line.split("\t")
        if len(parts) != 3:
            raise ApiPolicyError(f"unexpected live-catalog row: {raw_line!r}")
        schema, name, identity_arguments = parts
        identities.append(
            FunctionIdentity(
                schema=schema,
                name=name,
                identity_arguments=identity_arguments,
            )
        )
    return identities


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ApiPolicyError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def load_policy_text(text: str, source: str) -> dict[str, PolicyEntry]:
    try:
        document = json.loads(text, object_pairs_hook=_reject_duplicate_json_keys)
    except json.JSONDecodeError as exc:
        raise ApiPolicyError(f"invalid JSON in {source}: {exc}") from exc

    if not isinstance(document, dict):
        raise ApiPolicyError(f"policy root in {source} must be a JSON object")
    functions = document.get("functions")
    if not isinstance(functions, dict):
        raise ApiPolicyError(f"policy {source} must contain a 'functions' object")

    entries: dict[str, PolicyEntry] = {}
    for key, value in functions.items():
        if not isinstance(value, dict):
            raise ApiPolicyError(f"policy entry {key!r} in {source} must be an object")

        schema = value.get("schema")
        name = value.get("name")
        identity_arguments = value.get("identity_arguments")
        api_class = value.get("class")

        if not all(isinstance(field, str) for field in (schema, name, identity_arguments, api_class)):
            raise ApiPolicyError(
                f"policy entry {key!r} in {source} must define string schema/name/identity_arguments/class fields"
            )
        if api_class not in API_CLASSES:
            raise ApiPolicyError(
                f"policy entry {key!r} in {source} uses unknown class {api_class!r}"
            )

        identity = FunctionIdentity(schema=schema, name=name, identity_arguments=identity_arguments)
        if key != identity.key:
            raise ApiPolicyError(
                f"policy key {key!r} does not match its explicit identity {identity.key!r} in {source}"
            )
        if key in entries:
            raise ApiPolicyError(f"duplicate policy entry for {key} in {source}")

        entries[key] = PolicyEntry(identity=identity, api_class=api_class)

    return entries


def load_policy(path: Path) -> dict[str, PolicyEntry]:
    if not path.is_file():
        raise ApiPolicyError(f"policy file not found: {path}")
    return load_policy_text(path.read_text(encoding="utf-8"), str(path))


def suggest_classification(identity: FunctionIdentity) -> SuggestedClassification:
    name = identity.name
    if name in _TRIGGER_ENTRY_NAMES:
        return SuggestedClassification("trigger_entry", "event-trigger entrypoint")
    if name in _INTERNAL_NAMES or name.startswith("_") or name.startswith("pgt_ivm_"):
        return SuggestedClassification("internal", "internal callback or extension-owned helper")
    if name in _ARBITRARY_SQL_NAMES:
        return SuggestedClassification("arbitrary_sql", "executes caller-supplied SQL")
    if name in _ADMIN_GLOBAL_NAMES:
        return SuggestedClassification("admin_global", "cluster or scheduler administration")
    if name in _OWNER_LIFECYCLE_NAMES or name.startswith(_OWNER_LIFECYCLE_PREFIXES):
        return SuggestedClassification("owner_lifecycle", "stream-table lifecycle mutation")
    return SuggestedClassification("public_read", "read-only diagnostics or inspection")


def build_generated_policy(
    identities: Iterable[FunctionIdentity],
    source_label: str,
) -> dict[str, object]:
    functions: dict[str, object] = {}
    for identity in sorted({identity.key: identity for identity in identities}.values()):
        functions[identity.key] = {
            "schema": identity.schema,
            "name": identity.name,
            "identity_arguments": identity.identity_arguments,
            "class": suggest_classification(identity).api_class,
        }
    return {
        "policy_version": 1,
        "generated_from": source_label,
        "functions": functions,
    }


def validate_policy(
    source_identities: Iterable[FunctionIdentity],
    policy_entries: dict[str, PolicyEntry],
) -> ValidationReport:
    source_counts = Counter(identity.key for identity in source_identities)
    duplicate_source_signatures = tuple(sorted(key for key, count in source_counts.items() if count > 1))
    source_signatures = set(source_counts)
    policy_signatures = set(policy_entries)
    missing_policy_signatures = tuple(sorted(source_signatures - policy_signatures))
    extra_policy_signatures = tuple(sorted(policy_signatures - source_signatures))
    return ValidationReport(
        duplicate_source_signatures=duplicate_source_signatures,
        missing_policy_signatures=missing_policy_signatures,
        extra_policy_signatures=extra_policy_signatures,
    )


def emit_validation_report(report: ValidationReport) -> int:
    if report.duplicate_source_signatures:
        print("ERROR: duplicate function signatures discovered in inspected source:")
        for signature in report.duplicate_source_signatures:
            print(f"  - {signature}")
        print()

    if report.missing_policy_signatures:
        print("ERROR: missing explicit policy entries for these overloads:")
        for signature in report.missing_policy_signatures:
            identity = parse_identity_key(signature)
            suggestion = suggest_classification(identity)
            print(
                f"  - {signature}  -> suggested class {suggestion.api_class}"
                f" ({suggestion.rationale})"
            )
        print()

    if report.extra_policy_signatures:
        print("ERROR: policy entries exist for signatures not found in the inspected source:")
        for signature in report.extra_policy_signatures:
            print(f"  - {signature}")
        print()

    if report.ok:
        return 0
    return 1


def parse_identity_key(key: str) -> FunctionIdentity:
    match = re.fullmatch(r"([^\.]+)\.([^\(]+)\((.*)\)", key)
    if not match:
        raise ApiPolicyError(f"invalid identity key: {key!r}")
    return FunctionIdentity(
        schema=match.group(1),
        name=match.group(2),
        identity_arguments=match.group(3),
    )


def emit_acl_sql(policy_entries: dict[str, PolicyEntry]) -> str:
    lines = [
        "-- Generated by scripts/check_sql_api_policy.py emit-acl-sql",
        "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle FROM PUBLIC;",
        "",
        "-- Explicit overload policy follows.",
    ]

    grouped: dict[str, list[PolicyEntry]] = {api_class: [] for api_class in sorted(API_CLASSES)}
    for entry in policy_entries.values():
        grouped[entry.api_class].append(entry)

    for api_class in sorted(grouped):
        entries = sorted(grouped[api_class], key=lambda entry: entry.identity)
        if not entries:
            continue
        lines.append("")
        lines.append(f"-- {api_class}")
        for entry in entries:
            if api_class == "public_read":
                lines.append(
                    f"GRANT EXECUTE ON FUNCTION {entry.identity.sql_signature} TO PUBLIC;"
                )
            else:
                lines.append(
                    f"REVOKE EXECUTE ON FUNCTION {entry.identity.sql_signature} FROM PUBLIC;"
                )
    lines.append("")
    return "\n".join(lines)


class ScriptSelfTests(unittest.TestCase):
    def test_parse_packaged_sql_functions_canonicalizes_identity_args(self) -> None:
        sql = """
CREATE FUNCTION pgtrickle."foo"(
    "a" INT,
    "b" TEXT DEFAULT 'x',
    "c" bool DEFAULT false,
    "d" interval DEFAULT '5 minutes'::interval
) RETURNS void LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$;
CREATE FUNCTION pgtrickle."foo"("relid" oid) RETURNS void LANGUAGE c AS 'MODULE_PATHNAME', 'foo';
CREATE FUNCTION pgtrickle."bar"() RETURNS void LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$;
"""
        identities = parse_packaged_sql_functions(sql)
        self.assertEqual(
            [identity.key for identity in identities],
            [
                "pgtrickle.foo(integer, text, boolean, interval)",
                "pgtrickle.foo(oid)",
                "pgtrickle.bar()",
            ],
        )

    def test_duplicate_json_keys_are_rejected(self) -> None:
        policy = """
{
  "functions": {
    "pgtrickle.foo()": {"schema": "pgtrickle", "name": "foo", "identity_arguments": "", "class": "public_read"},
    "pgtrickle.foo()": {"schema": "pgtrickle", "name": "foo", "identity_arguments": "", "class": "public_read"}
  }
}
"""
        with self.assertRaises(ApiPolicyError):
            load_policy_text(policy, "<memory>")

    def test_validation_reports_missing_exact_signature(self) -> None:
        source = [FunctionIdentity("pgtrickle", "foo", "integer")]
        policy_text = """
{
  "functions": {
    "pgtrickle.bar()": {"schema": "pgtrickle", "name": "bar", "identity_arguments": "", "class": "public_read"}
  }
}
"""
        report = validate_policy(source, load_policy_text(policy_text, "<memory>"))
        self.assertEqual(report.missing_policy_signatures, ("pgtrickle.foo(integer)",))
        self.assertEqual(report.extra_policy_signatures, ("pgtrickle.bar()",))

    def test_emit_acl_sql_uses_exact_overloads(self) -> None:
        policy_text = """
{
  "functions": {
    "pgtrickle.foo()": {"schema": "pgtrickle", "name": "foo", "identity_arguments": "", "class": "public_read"},
    "pgtrickle.bar(text)": {"schema": "pgtrickle", "name": "bar", "identity_arguments": "text", "class": "owner_lifecycle"}
  }
}
"""
        sql = emit_acl_sql(load_policy_text(policy_text, "<memory>"))
        self.assertIn("GRANT EXECUTE ON FUNCTION pgtrickle.foo() TO PUBLIC;", sql)
        self.assertIn("REVOKE EXECUTE ON FUNCTION pgtrickle.bar(text) FROM PUBLIC;", sql)


def run_self_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ScriptSelfTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def load_source_identities(args: argparse.Namespace) -> tuple[list[FunctionIdentity], str]:
    if args.live_dsn:
        identities = load_live_catalog(args.live_dsn, args.extension_name, args.psql_bin)
        return identities, f"live catalog extension {args.extension_name!r}"

    sql_path = Path(args.source_sql) if args.source_sql else default_archive_sql_path()
    identities = load_packaged_sql(sql_path)
    return identities, str(sql_path)


def add_source_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source-sql",
        help="Inspect packaged SQL at this path. Defaults to sql/archive/pg_trickle--<Cargo version>.sql.",
    )
    parser.add_argument(
        "--live-dsn",
        help="Inspect a live PostgreSQL catalog through psql using this DSN instead of packaged SQL.",
    )
    parser.add_argument(
        "--extension-name",
        default="pg_trickle",
        help="Extension name for --live-dsn inspection (default: pg_trickle).",
    )
    parser.add_argument(
        "--psql-bin",
        default="psql",
        help="psql binary to use for --live-dsn inspection (default: psql).",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command")

    check_parser = subparsers.add_parser("check", help="Validate explicit policy coverage.")
    add_source_options(check_parser)
    check_parser.add_argument(
        "--policy",
        default=str(DEFAULT_POLICY_PATH),
        help=f"Policy JSON path (default: {DEFAULT_POLICY_PATH.relative_to(REPO_ROOT)}).",
    )

    generate_parser = subparsers.add_parser(
        "generate-template",
        help="Generate an explicit policy template with heuristic classifications.",
    )
    add_source_options(generate_parser)
    generate_parser.add_argument(
        "--output",
        default="-",
        help="Write generated JSON here, or '-' for stdout (default).",
    )

    emit_parser = subparsers.add_parser(
        "emit-acl-sql",
        help="Emit deny-first ACL SQL from the checked-in explicit policy.",
    )
    emit_parser.add_argument(
        "--policy",
        default=str(DEFAULT_POLICY_PATH),
        help=f"Policy JSON path (default: {DEFAULT_POLICY_PATH.relative_to(REPO_ROOT)}).",
    )

    subparsers.add_parser("self-test", help="Run focused script self-tests.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "check"

    try:
        if command == "self-test":
            return run_self_tests()

        if command == "generate-template":
            identities, source_label = load_source_identities(args)
            document = build_generated_policy(identities, source_label)
            rendered = json.dumps(document, indent=2, sort_keys=False) + "\n"
            if args.output == "-":
                sys.stdout.write(rendered)
            else:
                Path(args.output).write_text(rendered, encoding="utf-8")
            return 0

        if command == "emit-acl-sql":
            policy_entries = load_policy(Path(args.policy))
            sys.stdout.write(emit_acl_sql(policy_entries))
            return 0

        if command == "check":
            if not hasattr(args, "policy"):
                args.policy = str(DEFAULT_POLICY_PATH)
            if not hasattr(args, "source_sql"):
                args.source_sql = None
            if not hasattr(args, "live_dsn"):
                args.live_dsn = None
            if not hasattr(args, "extension_name"):
                args.extension_name = "pg_trickle"
            if not hasattr(args, "psql_bin"):
                args.psql_bin = "psql"
            identities, source_label = load_source_identities(args)
            policy_entries = load_policy(Path(args.policy))
            report = validate_policy(identities, policy_entries)
            if report.ok:
                print(
                    f"OK: {len({identity.key for identity in identities})} function overloads in {source_label} "
                    f"match {len(policy_entries)} explicit policy entries in {args.policy}."
                )
                return 0
            return emit_validation_report(report)

        parser.error(f"unknown command: {command}")
        return 2
    except ApiPolicyError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
