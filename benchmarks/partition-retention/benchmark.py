#!/usr/bin/env python3
"""Compare source reinitialization with an aligned TRIM partition removal."""

import argparse
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import uuid


def run(db, sql):
    return subprocess.run(
        ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", sql],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def internal_plan_summary(log, schema):
    decoder = json.JSONDecoder()
    plans = []
    position = 0
    marker = "plan:\n"
    while (start := log.find(marker, position)) != -1:
        start = log.find("{", start + len(marker))
        if start == -1:
            raise ValueError("auto_explain plan has no JSON object")
        plan, length = decoder.raw_decode(log[start:])
        position = start + length
        if "Plan" in plan:
            plans.append(plan)
    if not plans:
        raise ValueError("auto_explain did not emit any plans")
    relevant = []
    source_partitions = set()
    source_rows = 0
    output_rows = 0
    for plan in plans:
        scans = []
        stack = [plan["Plan"]]
        while stack:
            node = stack.pop()
            stack.extend(node.get("Plans", []))
            relation = node.get("Relation Name", "")
            kind = node.get("Node Type", "")
            if node.get("Schema") != schema or not kind.endswith("Scan") or not node.get("Actual Loops", 0):
                continue
            if relation != "totals" and not (relation.startswith("p") and relation[1:].isdigit()):
                continue
            rows = (node.get("Actual Rows", 0) + node.get("Rows Removed by Filter", 0)
                    + node.get("Rows Removed by Index Recheck", 0)) * node.get("Actual Loops", 0)
            condition = node.get("Index Cond") or node.get("Recheck Cond") or node.get("TID Cond")
            scans.append({"relation": relation, "node": kind, "rows_examined": rows,
                          "condition": condition})
            if relation == "totals":
                output_rows += rows
            else:
                source_partitions.add(relation)
                source_rows += rows
        if scans:
            relevant.append({"scans": scans, "plan": plan})
    return {"captured_statements": len(plans), "source_partitions_scanned": sorted(source_partitions),
            "source_rows_examined": source_rows, "output_rows_examined": output_rows,
            "relevant_plans": relevant}


def validate_trim_drop_plan(summary):
    retained = set(summary["source_partitions_scanned"]) - {"p0"}
    if retained:
        raise RuntimeError(f"TRIM scanned retained source partitions: {sorted(retained)}")
    if any(scan["relation"] == "totals" and not scan.get("condition")
           for plan in summary["relevant_plans"] for scan in plan["scans"]):
        raise RuntimeError("TRIM used an unbounded output scan")


def timed(db, sql, schema, instrument):
    start = time.perf_counter()
    if not instrument:
        result = run(db, sql)
        return result, (time.perf_counter() - start) * 1000, None
    settings = ("LOAD 'auto_explain'; SET client_min_messages = log; "
                "SET auto_explain.log_min_duration = 0; SET auto_explain.log_analyze = on; "
                "SET auto_explain.log_timing = off; SET auto_explain.log_nested_statements = on; "
                "SET auto_explain.log_verbose = on; SET auto_explain.log_format = json; ")
    result = subprocess.run(
        ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", settings + sql],
        check=True, capture_output=True, text=True,
    )
    elapsed = (time.perf_counter() - start) * 1000
    return result.stdout.strip(), elapsed, internal_plan_summary(result.stderr, schema)


def workload(db, directory, sql, application_name):
    directory.mkdir()
    script = directory / "workload.sql"
    script.write_text(sql, encoding="utf-8")
    log = (directory / "pgbench.out").open("w", encoding="utf-8")
    process = subprocess.Popen(
        ["pgbench", "-n", "-T", "3600", "-l", "-f", str(script), db],
        cwd=directory, stdout=log, stderr=subprocess.STDOUT,
        env={**os.environ, "PGAPPNAME": application_name},
    )
    return process, log


def stop_workload(process, output, directory):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    process.wait(timeout=60)
    output.close()
    latencies = []
    for path in directory.glob("pgbench_log.*"):
        for line in path.read_text(encoding="utf-8").splitlines():
            fields = line.split()
            if len(fields) >= 3 and fields[2].isdigit():
                latencies.append(int(fields[2]) / 1000)
    latencies.sort()
    if not latencies:
        raise RuntimeError(f"no pgbench latency samples in {directory}")
    return {"transactions": len(latencies), "p95_ms": latencies[int((len(latencies) - 1) * .95)],
            "max_ms": latencies[-1]}


def wait_for_workload_sessions(db, application_name, present):
    deadline = time.monotonic() + 30
    while True:
        count = int(run(db, "SELECT count(*) FROM pg_catalog.pg_stat_activity "
                        f"WHERE datname = current_database() AND application_name = '{application_name}'"))
        if (count > 0) == present:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"workload sessions did not reach {present=}: {application_name}")
        time.sleep(.05)


def refresh_until_consistent(db, table, defining):
    deadline = time.monotonic() + 30
    attempts = 0
    while True:
        attempts += 1
        run(db, f"SELECT pgtrickle.refresh_stream_table('{table}')")
        mismatch = int(run(db, f"SELECT count(*) FROM ((({defining}) EXCEPT ALL (SELECT d, account, total, n FROM {table})) UNION ALL ((SELECT d, account, total, n FROM {table}) EXCEPT ALL ({defining}))) AS differences"))
        if mismatch == 0:
            return attempts
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{table} did not converge after {attempts} refresh calls: {mismatch} mismatches")
        time.sleep(.1)


def trial(db, schema, events, trim, directory, instrument, downstream, keyless, pending_events, failure_plan):
    old = datetime.date(2025, 1, 1)
    partitions = []
    for day in range(31):
        lower = old + datetime.timedelta(days=day)
        upper = lower + datetime.timedelta(days=1)
        partitions.append(
            f"CREATE TABLE {schema}.p{day} PARTITION OF {schema}.events "
            f"FOR VALUES FROM ('{lower}') TO ('{upper}');"
        )
    primary_key = "" if keyless else ", PRIMARY KEY(id,d)"
    run(db, f"CREATE SCHEMA {schema}; CREATE TABLE {schema}.events "
        f"(id bigint NOT NULL, d date NOT NULL, account integer NOT NULL, amount integer NOT NULL{primary_key}) "
        "PARTITION BY RANGE(d); " + " ".join(partitions))
    expired = max(events // 30, 1000)
    run(db, f"INSERT INTO {schema}.events SELECT g, DATE '2025-01-01', "
        f"((g-1)%1000)::integer, 1 FROM generate_series(1,{expired}) g; "
        f"INSERT INTO {schema}.events SELECT g+{expired}, DATE '2025-01-02' + (((g-1)/1000)%30)::integer, "
        f"((g-1)%1000)::integer, 1 FROM generate_series(1,{events}) g; "
        f"CREATE SEQUENCE {schema}.ids START WITH {events + expired + 1}; ANALYZE {schema}.events")
    defining = f"SELECT d, account, sum(amount) AS total, count(*) AS n FROM {schema}.events GROUP BY d, account"
    run(db, f"SELECT pgtrickle.create_stream_table('{schema}.totals', $$ {defining} $$, '1h', 'DIFFERENTIAL')")
    if trim:
        run(db, f"SELECT pgtrickle.set_stream_table_partition_retention('{schema}.totals', 'TRIM', 'd', '2025-01-02')")
    if downstream:
        run(db, f"SELECT pgtrickle.create_stream_table('{schema}.downstream', "
            f"$$ SELECT d, account, total, n FROM {schema}.totals $$, '1h', 'DIFFERENTIAL')")
    source_plan = json.loads(run(db, f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {defining}"))
    output_plan = json.loads(run(db, f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM {schema}.totals WHERE d >= DATE '2025-01-01' AND d < DATE '2025-01-02'"))
    history_start = int(run(db, "SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history"))
    writer_dir, reader_dir = directory / "writer", directory / "reader"
    writer_name, reader_name = f"{schema}_writer", f"{schema}_reader"
    writer, writer_log = workload(db, writer_dir, f"\\set account random(0, 999)\nINSERT INTO {schema}.events VALUES (nextval('{schema}.ids'), DATE '2025-01-31', :account, 1);\n", writer_name)
    reader, reader_log = workload(db, reader_dir, f"SELECT sum(total) FROM {schema}.totals;\n", reader_name)
    try:
        wait_for_workload_sessions(db, writer_name, True)
        wait_for_workload_sessions(db, reader_name, True)
        time.sleep(1)
        if pending_events:
            run(db, f"INSERT INTO {schema}.events SELECT nextval('{schema}.ids'), "
                "DATE '2025-01-31', ((g-1)%1000)::integer, 1 "
                f"FROM generate_series(1,{pending_events}) g")
        _, drop_ms, drop_internal = timed(db, f"DROP TABLE {schema}.p0", schema, instrument)
        _, refresh_ms, refresh_internal = timed(db, f"SELECT pgtrickle.refresh_stream_table('{schema}.totals')", schema, instrument)
    finally:
        try:
            writer_latency = stop_workload(writer, writer_log, writer_dir)
        finally:
            reader_latency = stop_workload(reader, reader_log, reader_dir)
        wait_for_workload_sessions(db, writer_name, False)
        wait_for_workload_sessions(db, reader_name, False)
    drain_calls = refresh_until_consistent(db, f"{schema}.totals", defining)
    mismatch = 0
    downstream_mismatch = None
    downstream_history = None
    downstream_calls = None
    if downstream:
        downstream_calls = refresh_until_consistent(db, f"{schema}.downstream", defining)
        downstream_mismatch = 0
        downstream_history = json.loads(run(db, f"SELECT COALESCE(json_agg(json_build_object('action',h.action,'status',h.status,'fallback',h.was_full_fallback) ORDER BY h.refresh_id),'[]'::json) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_schema = '{schema}' AND s.pgt_name = 'downstream' AND h.refresh_id > {history_start}"))
        if downstream_mismatch or not downstream_history or any(h["status"] != "COMPLETED" for h in downstream_history):
            raise RuntimeError(f"incorrect downstream result or incomplete refresh: {downstream_mismatch=}, {downstream_history=}")
        if trim and (not any(h["action"] == "DIFFERENTIAL" for h in downstream_history) or
                     any(h["action"] in ("FULL", "REINITIALIZE") or h["fallback"] for h in downstream_history)):
            raise RuntimeError(f"downstream did not use differential maintenance: {downstream_history}")
    history = json.loads(run(db, f"SELECT COALESCE(json_agg(json_build_object('action',h.action,'status',h.status,'fallback',h.was_full_fallback,'duration_ms',h.duration_ms) ORDER BY h.refresh_id),'[]'::json) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_schema = '{schema}' AND s.pgt_name = 'totals' AND h.refresh_id > {history_start}"))
    if mismatch or not history or any(h["status"] != "COMPLETED" for h in history):
        raise RuntimeError(f"incorrect result or incomplete refresh: {mismatch=}, {history=}")
    if trim and (not any(h["action"] == "DIFFERENTIAL" for h in history) or
                 any(h["action"] in ("FULL", "REINITIALIZE") or h["fallback"] for h in history)):
        raise RuntimeError(f"TRIM did not use differential maintenance: {history}")
    if trim and instrument:
        try:
            validate_trim_drop_plan(drop_internal)
        except RuntimeError:
            failure_plan.write_text(json.dumps(drop_internal, indent=2) + "\n", encoding="utf-8")
            raise
    return {"mode": "TRIM" if trim else "baseline", "retained_events": events,
            "pending_events": pending_events,
            "keyless_source": keyless,
            "expired_events": expired, "drop_ms": drop_ms, "refresh_call_ms": refresh_ms,
            "writer": writer_latency, "reader": reader_latency, "refresh_history": history,
            "result_mismatches": mismatch, "drop_internal": drop_internal, "refresh_internal": refresh_internal,
            "drain_refresh_calls": drain_calls, "downstream_mismatches": downstream_mismatch,
            "downstream_refresh_calls": downstream_calls, "downstream_history": downstream_history,
            "source_query_plan": source_plan,
            "output_range_query_plan": output_plan}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, help="disposable PostgreSQL database connection string")
    parser.add_argument("--events", type=int, default=9_000_000)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--instrument-internal", action="store_true",
                        help="capture nested PostgreSQL plans with auto_explain (adds measurement overhead)")
    parser.add_argument("--downstream", action="store_true",
                        help="add a dependent stream and verify its results after refresh")
    parser.add_argument("--keyless", action="store_true",
                        help="create the partitioned source without a primary key")
    parser.add_argument("--pending-events", type=int, default=0,
                        help="insert this many additional events immediately before DROP")
    args = parser.parse_args()
    if args.events < 30_000:
        parser.error("--events must be at least 30000")
    if args.pending_events < 0:
        parser.error("--pending-events must be nonnegative")
    schemas = ["retention_bench_" + uuid.uuid4().hex[:12] for _ in range(2)]
    results = []
    with tempfile.TemporaryDirectory(prefix="pgtrickle-retention-") as tmp:
        try:
            for schema, trim in zip(schemas, (False, True)):
                directory = Path(tmp) / schema
                directory.mkdir()
                failure_plan = args.output.with_suffix(args.output.suffix + ".failed-plan.json")
                results.append(trial(args.database, schema, args.events, trim, directory, args.instrument_internal, args.downstream, args.keyless, args.pending_events, failure_plan))
                args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
        finally:
            for schema in schemas:
                run(args.database, f"DROP SCHEMA IF EXISTS {schema} CASCADE")


if __name__ == "__main__":
    main()
