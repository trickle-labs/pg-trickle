#!/usr/bin/env python3
"""Generate and diff deterministic live pg_trickle catalog manifests."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path


OWNER_TOKEN = "<extension_owner>"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate or diff a normalized live pg_trickle catalog manifest."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    dump_parser = subparsers.add_parser(
        "dump", help="Dump a normalized manifest from a live PostgreSQL database."
    )
    dump_parser.add_argument(
        "--database",
        help="psql connection string or database name. Defaults to normal libpq env resolution.",
    )
    dump_parser.add_argument(
        "--psql",
        default="psql",
        help="psql binary to invoke (default: %(default)s).",
    )
    dump_parser.add_argument(
        "--output",
        help="Write the manifest to this path instead of stdout.",
    )

    diff_parser = subparsers.add_parser(
        "diff", help="Diff two manifest JSON files and emit a machine-readable report."
    )
    diff_parser.add_argument("left", help="Left manifest JSON file.")
    diff_parser.add_argument("right", help="Right manifest JSON file.")
    diff_parser.add_argument(
        "--output",
        help="Write the diff JSON to this path instead of stdout.",
    )

    return parser.parse_args()


def run_psql_json_rows(psql: str, database: str | None, query: str) -> list[dict]:
    command = [psql, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1"]
    if database:
        command.extend(["--dbname", database])
    command.extend(["-c", query])

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())

    rows: list[dict] = []
    for line in completed.stdout.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def normalize_value(value: object, extension_owner: str) -> object:
    if isinstance(value, dict):
        return {key: normalize_value(inner, extension_owner) for key, inner in value.items()}
    if isinstance(value, list):
        return [normalize_value(inner, extension_owner) for inner in value]
    if value == extension_owner:
        return OWNER_TOKEN
    return value


def sorted_unique(sequence: Iterable[str]) -> list[str]:
    return sorted(dict.fromkeys(sequence))


ACL_ENTRY_SQL = """
'grantee', CASE
    WHEN ax.grantee = 0 THEN 'PUBLIC'
    ELSE pg_get_userbyid(ax.grantee)
END,
'grantor', CASE
    WHEN ax.grantor = 0 THEN 'PUBLIC'
    ELSE pg_get_userbyid(ax.grantor)
END,
'privilege', ax.privilege_type,
'is_grantable', ax.is_grantable
"""


def one(rows: list[dict], *, query_name: str) -> dict:
    if len(rows) != 1:
        raise RuntimeError(f"{query_name} returned {len(rows)} rows (expected exactly 1)")
    return rows[0]


def build_manifest(psql: str, database: str | None) -> dict[str, object]:
    extension_owner = one(
        run_psql_json_rows(
            psql,
            database,
            f"""
            SELECT json_build_object(
                'owner', pg_get_userbyid(ext.extowner)
            )
            FROM pg_catalog.pg_extension ext
            WHERE ext.extname = 'pg_trickle'
            """,
        ),
        query_name="extension owner query",
    )["owner"]

    schema_rows = run_psql_json_rows(
        psql,
        database,
        f"""
        WITH ext AS (
            SELECT oid
            FROM pg_catalog.pg_extension
            WHERE extname = 'pg_trickle'
        )
        SELECT json_build_object(
            'name', n.nspname,
            'owner', pg_get_userbyid(n.nspowner),
            'acl', COALESCE((
                SELECT json_agg(
                    json_build_object(
                        {ACL_ENTRY_SQL}
                    )
                    ORDER BY CASE
                                 WHEN ax.grantee = 0 THEN 'PUBLIC'
                                 ELSE pg_get_userbyid(ax.grantee)
                             END,
                             CASE
                                 WHEN ax.grantor = 0 THEN 'PUBLIC'
                                 ELSE pg_get_userbyid(ax.grantor)
                             END,
                             ax.privilege_type,
                             ax.is_grantable
                )
                FROM pg_catalog.aclexplode(
                    COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))
                ) AS ax
            ), '[]'::json)
        )
        FROM pg_catalog.pg_namespace n
        JOIN pg_catalog.pg_depend dep
          ON dep.classid = 'pg_namespace'::regclass
         AND dep.objid = n.oid
         AND dep.refclassid = 'pg_extension'::regclass
         AND dep.deptype = 'e'
        JOIN ext
          ON ext.oid = dep.refobjid
        ORDER BY n.nspname
        """,
    )

    relation_rows = run_psql_json_rows(
        psql,
        database,
        f"""
        WITH ext_relations AS (
            SELECT c.oid,
                   n.nspname AS schema_name,
                   c.relname,
                   c.relkind,
                   c.relpersistence,
                   c.relrowsecurity,
                   c.relforcerowsecurity,
                   pg_get_userbyid(c.relowner) AS owner,
                   COALESCE(c.relacl, pg_catalog.acldefault('r', c.relowner)) AS acl,
                   dep.deptype
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n
              ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_depend dep
              ON dep.classid = 'pg_class'::regclass
             AND dep.objid = c.oid
             AND dep.refclassid = 'pg_extension'::regclass
             AND dep.deptype = 'e'
            JOIN pg_catalog.pg_extension ext
              ON ext.oid = dep.refobjid
             AND ext.extname = 'pg_trickle'
            WHERE c.relkind <> 'i'
        )
        SELECT json_build_object(
            'schema', schema_name,
            'name', relname,
            'relkind', relkind,
            'persistence', relpersistence,
            'owner', owner,
            'row_security', relrowsecurity,
            'force_row_security', relforcerowsecurity,
            'dependency_type', deptype,
            'acl', COALESCE((
                SELECT json_agg(
                    json_build_object(
                        {ACL_ENTRY_SQL}
                    )
                    ORDER BY CASE
                                 WHEN ax.grantee = 0 THEN 'PUBLIC'
                                 ELSE pg_get_userbyid(ax.grantee)
                             END,
                             CASE
                                 WHEN ax.grantor = 0 THEN 'PUBLIC'
                                 ELSE pg_get_userbyid(ax.grantor)
                             END,
                             ax.privilege_type,
                             ax.is_grantable
                )
                FROM pg_catalog.aclexplode(acl) AS ax
            ), '[]'::json)
        )
        FROM ext_relations
        ORDER BY schema_name, relname
        """,
    )

    column_rows = run_psql_json_rows(
        psql,
        database,
        """
        WITH ext_relations AS (
            SELECT c.oid,
                   format('%I.%I', n.nspname, c.relname) AS relation_identity
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n
              ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_depend dep
              ON dep.classid = 'pg_class'::regclass
             AND dep.objid = c.oid
             AND dep.refclassid = 'pg_extension'::regclass
             AND dep.deptype = 'e'
            JOIN pg_catalog.pg_extension ext
              ON ext.oid = dep.refobjid
             AND ext.extname = 'pg_trickle'
            WHERE c.relkind IN ('r', 'p', 'm', 'v', 'f')
        )
        SELECT json_build_object(
            'relation', rel.relation_identity,
            'ordinal', a.attnum,
            'name', a.attname,
            'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
            'collation', CASE
                WHEN a.attcollation = 0 THEN NULL
                WHEN a.attcollation = t.typcollation THEN NULL
                ELSE pg_catalog.quote_ident(cn.nspname) || '.' || pg_catalog.quote_ident(co.collname)
            END,
            'is_nullable', NOT a.attnotnull,
            'default', pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true),
            'identity', NULLIF(a.attidentity, ''),
            'generated', NULLIF(a.attgenerated, '')
        )
        FROM ext_relations rel
        JOIN pg_catalog.pg_attribute a
          ON a.attrelid = rel.oid
         AND a.attnum > 0
         AND NOT a.attisdropped
        JOIN pg_catalog.pg_type t
          ON t.oid = a.atttypid
        LEFT JOIN pg_catalog.pg_attrdef ad
          ON ad.adrelid = a.attrelid
         AND ad.adnum = a.attnum
        LEFT JOIN pg_catalog.pg_collation co
          ON co.oid = a.attcollation
        LEFT JOIN pg_catalog.pg_namespace cn
          ON cn.oid = co.collnamespace
        ORDER BY rel.relation_identity, a.attnum
        """,
    )

    constraint_rows = run_psql_json_rows(
        psql,
        database,
        """
        WITH ext_relations AS (
            SELECT c.oid,
                   format('%I.%I', n.nspname, c.relname) AS relation_identity
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n
              ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_depend dep
              ON dep.classid = 'pg_class'::regclass
             AND dep.objid = c.oid
             AND dep.refclassid = 'pg_extension'::regclass
             AND dep.deptype = 'e'
            JOIN pg_catalog.pg_extension ext
              ON ext.oid = dep.refobjid
             AND ext.extname = 'pg_trickle'
        )
        SELECT json_build_object(
            'relation', rel.relation_identity,
            'name', con.conname,
            'type', con.contype,
            'definition', pg_catalog.pg_get_constraintdef(con.oid, true),
            'is_deferrable', con.condeferrable,
            'initially_deferred', con.condeferred,
            'is_validated', con.convalidated
        )
        FROM pg_catalog.pg_constraint con
        JOIN ext_relations rel
          ON rel.oid = con.conrelid
        ORDER BY rel.relation_identity, con.conname
        """,
    )

    index_rows = run_psql_json_rows(
        psql,
        database,
        """
        SELECT json_build_object(
            'schema', n.nspname,
            'name', idx.relname,
            'table', format('%I.%I', tn.nspname, tbl.relname),
            'access_method', am.amname,
            'is_unique', ind.indisunique,
            'is_primary', ind.indisprimary,
            'is_exclusion', ind.indisexclusion,
            'definition', pg_catalog.pg_get_indexdef(idx.oid, 0, true),
            'predicate', pg_catalog.pg_get_expr(ind.indpred, ind.indrelid, true),
            'is_valid', ind.indisvalid,
            'dependency_type', dep.deptype
        )
        FROM pg_catalog.pg_class idx
        JOIN pg_catalog.pg_namespace n
          ON n.oid = idx.relnamespace
        JOIN pg_catalog.pg_index ind
          ON ind.indexrelid = idx.oid
        JOIN pg_catalog.pg_class tbl
          ON tbl.oid = ind.indrelid
        JOIN pg_catalog.pg_namespace tn
          ON tn.oid = tbl.relnamespace
        JOIN pg_catalog.pg_am am
          ON am.oid = idx.relam
        JOIN pg_catalog.pg_depend dep
          ON dep.classid = 'pg_class'::regclass
         AND dep.objid = idx.oid
         AND dep.refclassid = 'pg_extension'::regclass
         AND dep.deptype = 'e'
        JOIN pg_catalog.pg_extension ext
          ON ext.oid = dep.refobjid
         AND ext.extname = 'pg_trickle'
        ORDER BY n.nspname, idx.relname
        """,
    )

    routine_rows = run_psql_json_rows(
        psql,
        database,
        """
        SELECT json_build_object(
            'schema', n.nspname,
            'name', p.proname,
            'identity_arguments', pg_catalog.pg_get_function_identity_arguments(p.oid),
            'kind', p.prokind,
            'result_type', pg_catalog.pg_get_function_result(p.oid),
            'language', l.lanname,
            'volatility', p.provolatile,
            'is_strict', p.proisstrict,
            'parallel', p.proparallel,
            'security_definer', p.prosecdef,
            'leakproof', p.proleakproof,
            'owner', pg_get_userbyid(p.proowner),
            'proconfig', COALESCE(to_json(p.proconfig), '[]'::json),
            'dependency_type', dep.deptype,
            'acl', COALESCE((
                SELECT json_agg(
                    json_build_object(
                        {ACL_ENTRY_SQL}
                    )
                    ORDER BY CASE
                                 WHEN ax.grantee = 0 THEN 'PUBLIC'
                                 ELSE pg_get_userbyid(ax.grantee)
                             END,
                             CASE
                                 WHEN ax.grantor = 0 THEN 'PUBLIC'
                                 ELSE pg_get_userbyid(ax.grantor)
                             END,
                             ax.privilege_type,
                             ax.is_grantable
                )
                FROM pg_catalog.aclexplode(
                    COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))
                ) AS ax
            ), '[]'::json)
        )
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n
          ON n.oid = p.pronamespace
        JOIN pg_catalog.pg_language l
          ON l.oid = p.prolang
        JOIN pg_catalog.pg_depend dep
          ON dep.classid = 'pg_proc'::regclass
         AND dep.objid = p.oid
         AND dep.refclassid = 'pg_extension'::regclass
         AND dep.deptype = 'e'
        JOIN pg_catalog.pg_extension ext
          ON ext.oid = dep.refobjid
         AND ext.extname = 'pg_trickle'
        ORDER BY n.nspname, p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid)
        """,
    )

    view_rows = run_psql_json_rows(
        psql,
        database,
        """
        SELECT json_build_object(
            'schema', n.nspname,
            'name', c.relname,
            'definition', pg_catalog.pg_get_viewdef(c.oid, true),
            'options', COALESCE(to_json(c.reloptions), '[]'::json)
        )
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n
          ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_depend dep
          ON dep.classid = 'pg_class'::regclass
         AND dep.objid = c.oid
         AND dep.refclassid = 'pg_extension'::regclass
         AND dep.deptype = 'e'
        JOIN pg_catalog.pg_extension ext
          ON ext.oid = dep.refobjid
         AND ext.extname = 'pg_trickle'
        WHERE c.relkind = 'v'
        ORDER BY n.nspname, c.relname
        """,
    )

    event_trigger_rows = run_psql_json_rows(
        psql,
        database,
        """
        SELECT json_build_object(
            'name', evt.evtname,
            'event', evt.evtevent,
            'enabled', evt.evtenabled,
            'tags', COALESCE(to_json(evt.evttags), '[]'::json),
            'function', format(
                '%I.%I(%s)',
                fn_ns.nspname,
                fn.proname,
                pg_catalog.pg_get_function_identity_arguments(fn.oid)
            ),
            'owner', pg_get_userbyid(evt.evtowner)
        )
        FROM pg_catalog.pg_event_trigger evt
        JOIN pg_catalog.pg_proc fn
          ON fn.oid = evt.evtfoid
        JOIN pg_catalog.pg_namespace fn_ns
          ON fn_ns.oid = fn.pronamespace
        JOIN pg_catalog.pg_depend dep
          ON dep.classid = 'pg_event_trigger'::regclass
         AND dep.objid = evt.oid
         AND dep.refclassid = 'pg_extension'::regclass
         AND dep.deptype = 'e'
        JOIN pg_catalog.pg_extension ext
          ON ext.oid = dep.refobjid
         AND ext.extname = 'pg_trickle'
        ORDER BY evt.evtname
        """,
    )

    membership_rows = run_psql_json_rows(
        psql,
        database,
        """
        SELECT json_build_object(
            'identity', pg_catalog.pg_describe_object(dep.classid, dep.objid, dep.objsubid),
            'dependency_type', dep.deptype
        )
        FROM pg_catalog.pg_depend dep
        JOIN pg_catalog.pg_extension ext
          ON ext.oid = dep.refobjid
         AND ext.extname = 'pg_trickle'
        WHERE dep.refclassid = 'pg_extension'::regclass
          AND dep.deptype = 'e'
        ORDER BY pg_catalog.pg_describe_object(dep.classid, dep.objid, dep.objsubid)
        """,
    )

    config_dump_rows = run_psql_json_rows(
        psql,
        database,
        """
        SELECT json_build_object(
            'relation', format('%I.%I', n.nspname, c.relname),
            'condition', cfg.condition
        )
        FROM pg_catalog.pg_extension ext
        JOIN LATERAL unnest(ext.extconfig, ext.extcondition) AS cfg(relid, condition)
          ON TRUE
        JOIN pg_catalog.pg_class c
          ON c.oid = cfg.relid
        JOIN pg_catalog.pg_namespace n
          ON n.oid = c.relnamespace
        WHERE ext.extname = 'pg_trickle'
        ORDER BY n.nspname, c.relname
        """,
    )

    manifest: dict[str, object] = {
        "schemas": {},
        "relations": {},
        "columns": {},
        "constraints": {},
        "indexes": {},
        "routines": {},
        "views": {},
        "event_triggers": {},
        "extension_membership": {},
        "config_dump_policy": {},
    }

    for row in schema_rows:
        normalized = normalize_value(row, extension_owner)
        manifest["schemas"][normalized["name"]] = normalized

    for row in relation_rows:
        normalized = normalize_value(row, extension_owner)
        key = f'{normalized["schema"]}.{normalized["name"]}'
        manifest["relations"][key] = normalized

    for row in column_rows:
        normalized = normalize_value(row, extension_owner)
        relation_key = normalized.pop("relation")
        manifest["columns"].setdefault(relation_key, {})
        manifest["columns"][relation_key][normalized["name"]] = normalized

    for row in constraint_rows:
        normalized = normalize_value(row, extension_owner)
        relation_key = normalized.pop("relation")
        manifest["constraints"].setdefault(relation_key, {})
        manifest["constraints"][relation_key][normalized["name"]] = normalized

    for row in index_rows:
        normalized = normalize_value(row, extension_owner)
        key = f'{normalized["schema"]}.{normalized["name"]}'
        manifest["indexes"][key] = normalized

    for row in routine_rows:
        normalized = normalize_value(row, extension_owner)
        key = (
            f'{normalized["schema"]}.{normalized["name"]}'
            f'({normalized["identity_arguments"]})'
        )
        manifest["routines"][key] = normalized

    for row in view_rows:
        normalized = normalize_value(row, extension_owner)
        options = sorted_unique(normalized.get("options", []))
        normalized["options"] = options
        normalized["security_barrier"] = "security_barrier=true" in options
        normalized["security_invoker"] = "security_invoker=true" in options
        key = f'{normalized["schema"]}.{normalized["name"]}'
        manifest["views"][key] = normalized

    for row in event_trigger_rows:
        normalized = normalize_value(row, extension_owner)
        normalized["tags"] = sorted_unique(normalized.get("tags", []))
        manifest["event_triggers"][normalized["name"]] = normalized

    for row in membership_rows:
        normalized = normalize_value(row, extension_owner)
        manifest["extension_membership"][normalized["identity"]] = normalized

    for row in config_dump_rows:
        normalized = normalize_value(row, extension_owner)
        manifest["config_dump_policy"][normalized["relation"]] = normalized

    return manifest


def compare(left: object, right: object, path: str = "$") -> dict[str, list[dict[str, object] | str]]:
    diff: dict[str, list[dict[str, object] | str]] = {
        "left_only": [],
        "right_only": [],
        "changed": [],
    }

    if type(left) is not type(right):
        diff["changed"].append({"path": path, "left": left, "right": right})
        return diff

    if isinstance(left, dict):
        for key in sorted(set(left) | set(right)):
            child_path = f"{path}.{key}"
            if key not in left:
                diff["right_only"].append(child_path)
                continue
            if key not in right:
                diff["left_only"].append(child_path)
                continue
            child = compare(left[key], right[key], child_path)
            diff["left_only"].extend(child["left_only"])
            diff["right_only"].extend(child["right_only"])
            diff["changed"].extend(child["changed"])
        return diff

    if isinstance(left, list):
        if left != right:
            diff["changed"].append({"path": path, "left": left, "right": right})
        return diff

    if left != right:
        diff["changed"].append({"path": path, "left": left, "right": right})
    return diff


def write_json(payload: object, output: str | None) -> None:
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if output:
        Path(output).write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)


def main() -> int:
    args = parse_args()

    if args.command == "dump":
        try:
            manifest = build_manifest(args.psql, args.database)
        except RuntimeError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        write_json(manifest, args.output)
        return 0

    if args.command == "diff":
        left = json.loads(Path(args.left).read_text(encoding="utf-8"))
        right = json.loads(Path(args.right).read_text(encoding="utf-8"))
        diff = compare(left, right)
        write_json(diff, args.output)
        return 0 if not any(diff.values()) else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
