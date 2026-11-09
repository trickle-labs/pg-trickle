#!/usr/bin/env python3
"""Compare batched live RAG corpus maintenance in an empty disposable database."""

import argparse
import json
import math
import re
import tarfile
import time

import fake_embedding_worker as worker
import live_rag_attestation as attestation
import live_rag_load
import measure_live_rag_recall as recall
import setup_live_rag


SEED = """
INSERT INTO embedding_demo.documents(id, body)
SELECT n, 'seed document ' || n FROM generate_series(1, :'rows'::integer) n;
INSERT INTO live_rag.access(document_id, tenant_id)
SELECT id, CASE WHEN id % 10 < 8 THEN 1 ELSE 2 + id % 19 END
FROM embedding_demo.documents;
INSERT INTO embedding_demo.results(document_id, incarnation, revision, chunk_version, model_version, embedding)
SELECT d.id, d.incarnation, d.revision, d.chunk_version, 'model-v1',
       ARRAY(SELECT ((((pg_catalog.hashint8(d.id * 8191 + i * 104729)::bigint & 2147483647) % 2001) - 1000)::real / 1000)
             FROM generate_series(1, :'dimensions'::integer) i ORDER BY i)::vector
FROM embedding_demo.documents d;
"""

CHANGED_IDS = "SELECT 1 + ((:'cycle'::bigint * :'batch'::bigint + i) % :'rows'::bigint) FROM generate_series(0, :'batch'::integer - 1) i"
CHANGE = """
BEGIN;
UPDATE embedding_demo.documents SET body = body || ' update ' || :'cycle'
WHERE id IN (""" + CHANGED_IDS + """);
SELECT embedding_demo.enqueue(id, 'model-v1') FROM embedding_demo.documents
WHERE id IN (""" + CHANGED_IDS + """);
COMMIT;
"""

MIXED_TARGETS = "SELECT 1 + :'cycle'::bigint * :'batch'::bigint + i AS id, i FROM generate_series(0, :'batch'::integer - 1) i"
MIXED_INSERTS = "SELECT :'rows'::bigint + :'cycle'::bigint * :'batch'::bigint + i + 1 AS id FROM generate_series(0, :'batch'::integer - 1) i WHERE i % 4 = 3"
MIXED_IDS = "SELECT id FROM (" + MIXED_TARGETS + ") target WHERE i % 4 <> 3 UNION ALL " + MIXED_INSERTS
MIXED_CHANGE = """
BEGIN;
UPDATE embedding_demo.documents SET body = body || ' update ' || :'cycle'
WHERE id IN (SELECT id FROM (""" + MIXED_TARGETS + """) target WHERE i % 4 = 0);
UPDATE live_rag.access SET visible = NOT visible
WHERE document_id IN (SELECT id FROM (""" + MIXED_TARGETS + """) target WHERE i % 4 = 1);
DELETE FROM embedding_demo.documents
WHERE id IN (SELECT id FROM (""" + MIXED_TARGETS + """) target WHERE i % 4 = 2);
INSERT INTO embedding_demo.documents(id, body)
SELECT id, 'new document ' || id FROM (""" + MIXED_INSERTS + """) inserted;
INSERT INTO live_rag.access(document_id, tenant_id)
SELECT id, CASE WHEN id % 10 < 8 THEN 1 ELSE 2 + id % 19 END FROM (""" + MIXED_INSERTS + """) inserted;
SELECT embedding_demo.enqueue(id, 'model-v1') FROM embedding_demo.documents
WHERE id IN (SELECT id FROM (""" + MIXED_TARGETS + """) target WHERE i % 4 = 0 UNION ALL """ + MIXED_INSERTS + """);
COMMIT;
"""

MAINTENANCE = {
    "application": "SELECT live_rag.sync_application(ARRAY({changed_ids})::bigint[]);",
    "periodic_full": "SELECT live_rag.recompute_full();",
    "differential": "SELECT pgtrickle.refresh_stream_table('live_rag.differential');",
}


def availability_sql(changed_ids: str) -> str:
    return """
WITH changed AS MATERIALIZED (""" + changed_ids + """)
SELECT json_build_object(
 'eligible', (SELECT count(*) FROM live_rag.truth WHERE id IN (SELECT id FROM changed)),
 'application', (SELECT count(*) FROM live_rag.application_current WHERE id IN (SELECT id FROM changed)),
 'periodic_full', (SELECT count(*) FROM live_rag.periodic_full_current WHERE id IN (SELECT id FROM changed)),
 'differential', (SELECT count(*) FROM live_rag.differential_current WHERE id IN (SELECT id FROM changed))
)::text;
"""

EXACT = """
SELECT json_build_object(
  'application', NOT EXISTS ((TABLE live_rag.application EXCEPT ALL TABLE live_rag.truth)
                             UNION ALL (TABLE live_rag.truth EXCEPT ALL TABLE live_rag.application)),
  'periodic_full', NOT EXISTS ((TABLE live_rag.periodic_full EXCEPT ALL TABLE live_rag.truth)
                               UNION ALL (TABLE live_rag.truth EXCEPT ALL TABLE live_rag.periodic_full)),
  'differential', NOT EXISTS ((SELECT id, tenant_id, body, incarnation, revision, chunk_version, model_version, embedding FROM live_rag.differential EXCEPT ALL TABLE live_rag.truth)
                              UNION ALL (TABLE live_rag.truth EXCEPT ALL SELECT id, tenant_id, body, incarnation, revision, chunk_version, model_version, embedding FROM live_rag.differential))
)::text;
"""


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summary(values: list[float]) -> dict:
    return {"samples": len(values), "p50": percentile(values, .50),
            "p95": percentile(values, .95), "p99": percentile(values, .99)}


def timed(sql: str, **variables: str) -> tuple[str, float]:
    start = time.monotonic()
    result = worker.psql(sql, **variables)
    return result, time.monotonic() - start


def json_result(output: str) -> dict:
    return json.loads(output.splitlines()[-1])


def reader_sql(dimensions: int) -> str:
    hybrid = recall.HYBRID_SQL.read_text(encoding="utf-8").strip()
    if not hybrid.endswith(";"):
        raise ValueError("hybrid query is invalid")
    hybrid = hybrid[:-1].replace("vector(3)", f"vector({dimensions})")
    return "WITH read_result AS MATERIALIZED (" + hybrid + """),
read_count AS MATERIALIZED (SELECT count(*) AS n FROM read_result)
SELECT json_build_object('server_started', extract(epoch FROM statement_timestamp()),
 'server_finished', extract(epoch FROM clock_timestamp()), 'rows', n)::text FROM read_count;
"""


def reader_summary(reads, client_window, begin, end, readers):
    completed = [read for read in reads if not read.get("timed_out")]
    timeouts = [read for read in reads if read.get("timed_out")]
    overlap = [read for read in completed if read["server_started"] < end and read["server_finished"] > begin]
    timeout_overlap = [read for read in timeouts if read["client_started"] < client_window[1] and read["client_finished"] > client_window[0]]
    return {"readers": readers, "queries": len(completed), "timed_out_queries": len(timeouts),
            "timeouts_overlapping_maintenance_client_window": len(timeout_overlap),
            "overlapping_queries": len(overlap),
            "client_latency_seconds": summary([read["client_seconds"] for read in completed]) if completed else None,
            "overlapping_client_latency_seconds": summary([read["client_seconds"] for read in overlap]) if overlap else None}


def run(args: argparse.Namespace) -> dict:
    metadata = json_result(worker.psql("""
SELECT json_build_object('postgres', current_setting('server_version'),
 'pgvector', (SELECT extversion FROM pg_extension WHERE extname='vector'),
 'pg_trickle', (SELECT extversion FROM pg_extension WHERE extname='pg_trickle'),
 'occupied', EXISTS (SELECT 1 FROM pg_namespace WHERE nspname IN ('live_rag', 'embedding_demo')))::text;
"""))
    if metadata["occupied"]:
        raise ValueError("live_rag or embedding_demo schema already exists; use an empty disposable database")
    if metadata["pg_trickle"] != "0.109.0" or metadata["pgvector"] not in ("0.8.1", "0.8.7") or not re.match(r"^18(?:\.|$)", metadata["postgres"]):
        raise ValueError("expected pg_trickle 0.109.0, PostgreSQL 18 and pgvector 0.8.1 or 0.8.7")

    def attest():
        identity = json_result(worker.psql(attestation.IDENTITY_SQL))
        return attestation.attest(args.attest_container, args.candidate_archive,
                                  args.package_sha256, args.candidate_commit, identity)

    initial_attestation = attest() if args.attest_container else None

    start = time.monotonic()
    worker.psql(setup_live_rag.setup_sql(args.dimensions))
    worker.psql(SEED, rows=str(args.rows), dimensions=str(args.dimensions))
    variables = {"rows": str(args.rows), "batch": str(args.batch), "cycle": "0"}
    worker.psql("SELECT live_rag.sync_application(ARRAY(SELECT id FROM live_rag.truth)::bigint[]); "
                "SELECT live_rag.recompute_full(); SELECT pgtrickle.refresh_stream_table('live_rag.differential');")
    worker.psql("ANALYZE embedding_demo.documents; ANALYZE embedding_demo.results; ANALYZE live_rag.access; "
                "ANALYZE live_rag.application; ANALYZE live_rag.periodic_full; ANALYZE live_rag.differential;")
    initial_seconds = time.monotonic() - start
    initial_exact = json_result(worker.psql(EXACT))
    if not all(initial_exact.values()):
        raise ValueError(f"initial corpora differ from truth: {initial_exact}")

    samples = []
    changed_ids = MIXED_IDS if args.workload == "mixed" else CHANGED_IDS
    change = MIXED_CHANGE if args.workload == "mixed" else CHANGE
    availability_query = availability_sql(changed_ids)
    for cycle in range(args.cycles):
        variables["cycle"] = str(cycle)
        start = time.monotonic()
        worker.psql(change, **variables)
        committed = time.monotonic()
        after_commit = json_result(worker.psql(availability_query, **variables))
        generation_start = time.monotonic()
        generation = worker.drain("model-v1", 3600, args.delay_seconds, args.batch, False, args.dimensions)
        eligible = time.monotonic()
        expected_jobs = (args.batch + 3) // 4 + args.batch // 4 if args.workload == "mixed" else args.batch
        if generation["accepted"] != expected_jobs or generation["stale"]:
            raise ValueError(f"unexpected embedding generation result: {generation}")
        after_generation = json_result(worker.psql(availability_query, **variables))
        names = list(MAINTENANCE)
        names = names[cycle % len(names):] + names[:cycle % len(names)]
        maintenance = {}
        for name in names:
            sql = "CREATE TEMP TABLE bench_wal AS SELECT pg_current_wal_insert_lsn() AS start_lsn, clock_timestamp() AS started;\n" + MAINTENANCE[name].replace("{changed_ids}", changed_ids) + "\nSELECT json_build_object('cluster_wal_bytes', pg_wal_lsn_diff(pg_current_wal_insert_lsn(), start_lsn), 'server_started', extract(epoch FROM started), 'server_finished', extract(epoch FROM clock_timestamp()))::text FROM bench_wal;"
            if args.concurrent_readers:
                reader_variables = {"corpus": f"{name}_current", "tenant": str(args.tenant),
                                    "query_vector": worker.embedding("seed document 1", "model-v1", args.dimensions),
                                    "terms": "seed"}
                (output, seconds), reads, client_window = live_rag_load.measure_during(
                    lambda: timed(sql, **variables), reader_sql(args.dimensions), reader_variables, args.concurrent_readers)
            else:
                output, seconds = timed(sql, **variables)
                reads = []
            maintenance[name] = {"duration_seconds": seconds, "eligible_to_searchable_seconds": time.monotonic() - eligible,
                                 **json_result(output)}
            if args.concurrent_readers:
                begin, end = maintenance[name]["server_started"], maintenance[name]["server_finished"]
                maintenance[name]["reader_load"] = reader_summary(reads, client_window, begin, end, args.concurrent_readers)
            maintenance[name]["availability"] = json_result(worker.psql(availability_query, **variables))
        exact = json_result(worker.psql(EXACT))
        if not all(exact.values()):
            raise ValueError(f"corpora differ from truth after cycle {cycle}: {exact}")
        retrieval = {}
        for name in MAINTENANCE:
            corpus = f"{name}_current"
            output, seconds = timed(recall.measurement_sql(args.dimensions), corpus=corpus,
                                    tenant=str(args.tenant), query_vector=worker.embedding("seed document 1", "model-v1", args.dimensions),
                                    terms="seed", k="10", iterative_scan="strict_order")
            retrieval[name] = {"measurement_command_seconds": seconds, **recall.parse_output(output, corpus)}
        refresh = json_result(worker.psql("""
SELECT row_to_json(h)::text FROM (
 SELECT h.action, h.status, h.was_full_fallback, h.refresh_reason, h.refresh_reason_detail, h.merge_strategy_used
 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id)
 WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1
) h;
"""))
        samples.append({"cycle": cycle, "order": names, "source_transaction_seconds": committed - start,
                        "commit_to_eligible_seconds": eligible - committed, "generation_seconds": eligible - generation_start,
                        "generation": generation,
                        "availability_after_commit": after_commit, "availability_after_generation": after_generation,
                        "maintenance": maintenance, "differential_refresh": refresh, "corpora_exact": exact,
                        "retrieval": retrieval})

    sizes = json_result(worker.psql("""
SELECT json_build_object('application', pg_total_relation_size('live_rag.application'),
 'periodic_full', pg_total_relation_size('live_rag.periodic_full'),
 'differential', pg_total_relation_size('live_rag.differential'))::text;
"""))
    final_attestation = attest() if initial_attestation else None
    if initial_attestation and initial_attestation != final_attestation:
        raise ValueError("candidate or PostgreSQL instance changed during the benchmark")
    return {"metadata": metadata, "candidate_commit": args.candidate_commit, "package_sha256": args.package_sha256,
            "identity_source": "archive manifest and installed files checked before and after run" if initial_attestation else "operator-declared; installed payload not attested",
            "attestation": initial_attestation, "rows": args.rows,
            "dimensions": args.dimensions, "batch": args.batch, "cycles": args.cycles, "workload": args.workload,
            "delay_seconds_per_job": args.delay_seconds, "concurrent_readers": args.concurrent_readers, "retrieval_tenant": args.tenant, "initial_setup_seconds": initial_seconds,
            "initial_corpora_exact": initial_exact, "samples": samples, "relation_bytes_including_indexes": sizes,
            "batch_latency_seconds": {"commit_to_eligible": summary([s["commit_to_eligible_seconds"] for s in samples]),
              "generation": summary([s["generation_seconds"] for s in samples]),
              **{name: summary([s["maintenance"][name]["duration_seconds"] for s in samples]) for name in MAINTENANCE}},
            "notes": "Deterministic hash-derived synthetic initial backfill bypasses claims; changed revisions use the fake worker. Timings include psql startup; commit-to-eligible also includes the post-commit availability check. Approaches run serially in rotating order; eligible-to-searchable includes preceding approaches and availability checks. WAL is cluster-wide insert-LSN movement around maintenance; other activity can contaminate it. Retrieval and optional concurrent reads use administrative current views, not restricted-role latency. Reader overlap uses database timestamps and client latencies include psql startup. No concurrent writes or embedding-delay distribution."}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--dimensions", type=int, choices=(384, 768, 1536), required=True)
    parser.add_argument("--batch", type=int, default=10)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=0)
    parser.add_argument("--tenant", type=int, default=1)
    parser.add_argument("--workload", choices=("updates", "mixed"), default="updates")
    parser.add_argument("--concurrent-readers", type=int, default=0, help="bounded hybrid readers per maintenance step (0-4)")
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--package-sha256", required=True)
    parser.add_argument("--candidate-archive", help="candidate release archive for Docker payload attestation")
    parser.add_argument("--attest-container", help="Docker container running the benchmark database")
    args = parser.parse_args()
    if bool(args.candidate_archive) != bool(args.attest_container):
        parser.error("--candidate-archive and --attest-container must be supplied together")
    if not 1 <= args.rows <= 1_000_000 or not 1 <= args.batch <= min(args.rows, 1000) or not 1 <= args.cycles <= 1000 or not 0 <= args.delay_seconds <= 10 or not math.isfinite(args.delay_seconds) or not 1 <= args.tenant <= 20 or not 0 <= args.concurrent_readers <= 4:
        parser.error("rows, batch, cycles, delay or tenant is outside the supported range")
    if not re.fullmatch(r"[0-9a-f]{40}", args.candidate_commit) or not re.fullmatch(r"[0-9a-f]{64}", args.package_sha256):
        parser.error("candidate commit and package SHA-256 must be lowercase hexadecimal")
    if args.workload == "mixed" and (args.batch < 4 or args.batch * args.cycles > args.rows):
        parser.error("mixed workload requires batch >= 4 and rows >= batch * cycles so deleted IDs are not reused")
    try:
        print(json.dumps(run(args), sort_keys=True))
    except (OSError, ValueError, KeyError, tarfile.TarError, json.JSONDecodeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
