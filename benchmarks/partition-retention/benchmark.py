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
    try:
        return subprocess.run(
            ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", sql],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"PostgreSQL command failed: {error.stderr.strip()}") from None


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
            if not (relation == "totals" or relation.startswith("totals_") or
                    (relation.startswith("p") and relation[1:].isdigit())):
                continue
            rows = (node.get("Actual Rows", 0) + node.get("Rows Removed by Filter", 0)
                    + node.get("Rows Removed by Index Recheck", 0)) * node.get("Actual Loops", 0)
            condition = node.get("Index Cond") or node.get("Recheck Cond") or node.get("TID Cond")
            scans.append({"relation": relation, "node": kind, "rows_examined": rows,
                          "condition": condition})
            if relation == "totals" or relation.startswith("totals_"):
                output_rows += rows
            else:
                source_partitions.add(relation)
                source_rows += rows
        if scans:
            relevant.append({"scans": scans, "plan": plan})
    return {"captured_statements": len(plans), "source_partitions_scanned": sorted(source_partitions),
            "source_rows_examined": source_rows, "output_rows_examined": output_rows,
            "relevant_plans": relevant}


def validate_trim_drop_plan(summary, affected_group_limit=None):
    retained = set(summary["source_partitions_scanned"]) - {"p0"}
    if retained:
        raise RuntimeError(f"TRIM scanned retained source partitions: {sorted(retained)}")
    if any((scan["relation"] == "totals" or scan["relation"].startswith("totals_")) and not scan.get("condition")
           for plan in summary["relevant_plans"] for scan in plan["scans"]):
        raise RuntimeError("TRIM used an unbounded output scan")
    if affected_group_limit is not None and any(
            (scan["relation"] == "totals" or scan["relation"].startswith("totals_"))
            and scan["rows_examined"] > 2 * affected_group_limit
            for plan in summary["relevant_plans"] for scan in plan["scans"]):
        raise RuntimeError("TRIM output scan exceeded twice the affected-group bound")
    excessive_pairs = False
    for entry in summary["relevant_plans"]:
        plan = entry.get("plan", {})
        if "Plan" not in plan:
            continue
        stack = [plan["Plan"]]
        while stack:
            node = stack.pop()
            stack.extend(node.get("Plans", []))
            if (node.get("Node Type") == "Seq Scan"
                    and node.get("Relation Name", "").startswith("__pgt_delta_")
                    and node.get("Actual Loops", 0) > 1):
                raise RuntimeError("TRIM repeatedly scanned the affected delta")
            if (node.get("Node Type") == "Nested Loop"
                    and node.get("Rows Removed by Join Filter", 0)
                    > max(1000, 100 * node.get("Actual Rows", 0))):
                excessive_pairs = True
    if excessive_pairs:
        raise RuntimeError("TRIM compared excessive row pairs")


def validate_baseline_refresh(history, summary):
    if not any(h["action"] == "REINITIALIZE" and h["status"] == "COMPLETED" for h in history):
        raise RuntimeError("baseline did not complete a reinitialization")
    if summary is not None and set(summary["source_partitions_scanned"]) != {f"p{i}" for i in range(1, 31)}:
        raise RuntimeError("timed baseline refresh did not scan every retained partition")


def validate_partitioned_capture_pruning(summary, expected_partition):
    observed = set()
    statements = set()
    for entry in summary["relevant_plans"]:
        query = entry.get("plan", {}).get("Query Text", "")
        kind = "pre" if "INSERT INTO __pgt_pre_" in query else (
            "capture" if "changes_pgt_" in query else None)
        if kind is None:
            continue
        scans = [scan for scan in entry["scans"] if scan["relation"].startswith("totals_")]
        if scans:
            statements.add(kind)
            observed.update(scan["relation"] for scan in scans)
    if statements != {"pre", "capture"} or observed != {expected_partition}:
        raise RuntimeError(f"partitioned retention capture did not prune to {expected_partition}: "
                           f"{statements=}, {observed=}")


def timed(db, sql, schema, instrument):
    start = time.perf_counter()
    if not instrument:
        result = run(db, sql)
        return result, (time.perf_counter() - start) * 1000, None
    settings = ("LOAD 'auto_explain'; SET client_min_messages = log; "
                "SET auto_explain.log_min_duration = 0; SET auto_explain.log_analyze = on; "
                "SET auto_explain.log_timing = off; SET auto_explain.log_nested_statements = on; "
                "SET auto_explain.log_verbose = on; SET auto_explain.log_format = json; ")
    try:
        result = subprocess.run(
            ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", settings + sql],
            check=True, capture_output=True, text=True,
        )
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"PostgreSQL command failed: {error.stderr.strip()}") from None
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


def summarize_lock_samples(lines):
    samples = [json.loads(line) for line in lines if line.startswith("{")]
    if not samples:
        raise RuntimeError("lock sampler did not record any observations")
    summary = {name: {"lock_samples": 0, "missing_samples": 0,
                      "max_consecutive_sample_span_ms": 0.0, "wait_events": {}}
               for name in ("writer", "reader")}
    started = {name: None for name in summary}
    previous = None
    for sample in samples:
        timestamp = float(sample["ts"])
        if previous is not None and timestamp < previous:
            raise RuntimeError("lock sampler clock moved backwards")
        previous = timestamp
        for name, result in summary.items():
            activity = sample[name]
            if activity is None:
                result["missing_samples"] += 1
            if activity is not None and activity["wait_event_type"] == "Lock":
                event = activity["wait_event"]
                result["lock_samples"] += 1
                result["wait_events"][event] = result["wait_events"].get(event, 0) + 1
                if started[name] is None:
                    started[name] = timestamp
                result["max_consecutive_sample_span_ms"] = max(
                    result["max_consecutive_sample_span_ms"], (timestamp - started[name]) * 1000)
            else:
                started[name] = None
    return {"samples": len(samples), **summary}


def start_lock_sampler(db, directory, writer_name, reader_name):
    directory.mkdir()
    path = directory / "activity.jsonl"
    output = path.open("w", encoding="utf-8")
    try:
        process = subprocess.Popen(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", db],
                                   stdin=subprocess.PIPE, stdout=output, stderr=subprocess.STDOUT,
                                   text=True)
    except BaseException:
        output.close()
        raise
    def activity(name):
        escaped = name.replace("'", "''")
        return ("(SELECT row_to_json(a) FROM (SELECT wait_event_type, wait_event "
                "FROM pg_catalog.pg_stat_activity WHERE datname = current_database() "
                f"AND application_name = '{escaped}') a)")
    try:
        process.stdin.write("SELECT json_build_object('ts',extract(epoch from clock_timestamp()), "
                            f"'writer',{activity(writer_name)},'reader',{activity(reader_name)})::text\n"
                            "\\watch 0.01\n")
        process.stdin.flush()
        deadline = time.monotonic() + 30
        while path.stat().st_size == 0:
            if process.poll() is not None:
                raise RuntimeError("lock sampler exited before its first observation")
            if time.monotonic() >= deadline:
                raise RuntimeError("lock sampler did not start within 30 seconds")
            time.sleep(.01)
    except BaseException:
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        finally:
            try:
                process.stdin.close()
            except BrokenPipeError:
                pass
            finally:
                output.close()
        raise
    return process, output, path


def stop_lock_sampler(process, output, path):
    unexpected_exit = process.poll() is not None
    if not unexpected_exit:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise
    finally:
        try:
            process.stdin.close()
        except BrokenPipeError:
            pass
        finally:
            output.close()
    if unexpected_exit:
        tail = "\n".join(path.read_text(encoding="utf-8").splitlines()[-5:])
        raise RuntimeError(f"lock sampler exited early: {tail}")
    summary = summarize_lock_samples(path.read_text(encoding="utf-8").splitlines())
    if any(summary[name]["missing_samples"] for name in ("writer", "reader")):
        raise RuntimeError("lock sampler lost a workload session")
    return summary


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


def trial(db, schema, events, trim, directory, instrument, downstream, keyless, pending_events, partitioned_output, failure_plan, sample_lock_waits):
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
    partition_clause = ", partition_by => 'HASH:d'" if partitioned_output else ""
    run(db, f"SELECT pgtrickle.create_stream_table('{schema}.totals', $$ {defining} $$, '1h', 'DIFFERENTIAL'{partition_clause})")
    if trim:
        run(db, f"SELECT pgtrickle.set_stream_table_partition_retention('{schema}.totals', 'TRIM', 'd', '2025-01-02')")
    if downstream:
        run(db, f"SELECT pgtrickle.create_stream_table('{schema}.downstream', "
            f"$$ SELECT d, account, total, n FROM {schema}.totals $$, '1h', 'DIFFERENTIAL')")
    source_plan = json.loads(run(db, f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {defining}"))
    output_plan = json.loads(run(db, f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM {schema}.totals WHERE d >= DATE '2025-01-01' AND d < DATE '2025-01-02'"))
    # Only the expired day and the writer's live day can change in this workload.
    affected_group_limit = int(run(db, f"SELECT count(*) FROM {schema}.totals WHERE d IN (DATE '2025-01-01', DATE '2025-01-31')"))
    expected_capture_partition = None
    if trim and downstream and partitioned_output:
        expected_capture_partition = run(db, f"SELECT c.relname FROM {schema}.totals t JOIN pg_catalog.pg_class c ON c.oid=t.tableoid WHERE t.d=DATE '2025-01-31' LIMIT 1")
        if not expected_capture_partition:
            raise RuntimeError("could not identify the writer's output partition")
    history_start = int(run(db, "SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history"))
    writer_dir, reader_dir = directory / "writer", directory / "reader"
    writer_name, reader_name = f"{schema}_writer", f"{schema}_reader"
    writer, writer_log = workload(db, writer_dir, f"\\set account random(0, 999)\nINSERT INTO {schema}.events VALUES (nextval('{schema}.ids'), DATE '2025-01-31', :account, 1);\n", writer_name)
    reader, reader_log = workload(db, reader_dir, f"SELECT sum(total) FROM {schema}.totals;\n", reader_name)
    sampler = None
    lock_samples = None
    try:
        wait_for_workload_sessions(db, writer_name, True)
        wait_for_workload_sessions(db, reader_name, True)
        if sample_lock_waits:
            sampler = start_lock_sampler(db, directory / "sampler", writer_name, reader_name)
        time.sleep(1)
        if pending_events:
            run(db, f"INSERT INTO {schema}.events SELECT nextval('{schema}.ids'), "
                "DATE '2025-01-31', ((g-1)%1000)::integer, 1 "
                f"FROM generate_series(1,{pending_events}) g")
        _, drop_ms, drop_internal = timed(db, f"DROP TABLE {schema}.p0", schema, instrument)
        _, refresh_ms, refresh_internal = timed(db, f"SELECT pgtrickle.refresh_stream_table('{schema}.totals')", schema, instrument)
    finally:
        try:
            if sampler is not None:
                lock_samples = stop_lock_sampler(*sampler)
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
    if not trim:
        validate_baseline_refresh(history, refresh_internal)
    if trim and instrument:
        try:
            validate_trim_drop_plan(drop_internal, affected_group_limit)
            if expected_capture_partition is not None:
                validate_partitioned_capture_pruning(drop_internal, expected_capture_partition)
        except RuntimeError:
            failure_plan.write_text(json.dumps({"affected_group_limit": affected_group_limit,
                                                "drop_internal": drop_internal}, indent=2) + "\n", encoding="utf-8")
            raise
    return {"mode": "TRIM" if trim else "baseline", "retained_events": events,
            "pending_events": pending_events,
            "partitioned_output": partitioned_output,
            "keyless_source": keyless,
            "expired_events": expired, "drop_ms": drop_ms, "refresh_call_ms": refresh_ms,
            "affected_group_limit": affected_group_limit,
            "expected_capture_partition": expected_capture_partition,
            "writer": writer_latency, "reader": reader_latency, "refresh_history": history,
            "lock_samples": lock_samples,
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
    parser.add_argument("--partitioned-output", action="store_true",
                        help="hash-partition the stream table output by its date group")
    parser.add_argument("--sample-lock-waits", action="store_true",
                        help="sample reader and writer heavyweight lock waits every 10 ms")
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
                results.append(trial(args.database, schema, args.events, trim, directory, args.instrument_internal, args.downstream, args.keyless, args.pending_events, args.partitioned_output, failure_plan, args.sample_lock_waits))
                args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
        finally:
            for schema in schemas:
                run(args.database, f"DROP SCHEMA IF EXISTS {schema} CASCADE")


if __name__ == "__main__":
    main()
