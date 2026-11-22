#!/usr/bin/env python3
"""Compare batched live RAG corpus maintenance in an empty disposable database."""

import argparse
import atexit
import json
import math
import os
import re
import tarfile
import time
from pathlib import Path

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

CHANGED_IDS = "SELECT 1 + ((:'cycle'::bigint * :'batch'::bigint + i) % :'rows'::bigint) AS id FROM generate_series(0, :'batch'::integer - 1) i"
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
    "periodic_full": "SELECT live_rag.recompute_full(:'dimensions'::integer);",
    "differential": "SELECT pgtrickle.refresh_stream_table('live_rag.differential');",
}
SCHEDULER_STREAMS = "ARRAY['embedding_demo.corpus_v1', 'embedding_demo.corpus_v2', 'live_rag.differential']"


class CheckpointWriter:
    def __init__(self, path: Path | None):
        self.path = path
        self.document = {}

    def update(self, progress: dict, **fields) -> None:
        if self.path is None:
            return
        self.document.update(fields)
        self.document.update(
            status="running",
            progress=progress,
            checkpoint_updated_at=time.time(),
        )
        self._write()

    def complete(self, report: dict) -> None:
        if self.path is None:
            return
        self.document = {
            **report,
            "status": "complete",
            "progress": {"phase": "complete"},
            "checkpoint_updated_at": time.time(),
        }
        self._write()

    def fail(self, error: Exception) -> None:
        if self.path is None:
            return
        self.document.update(
            status="failed",
            error=f"{type(error).__name__}: {error}",
            checkpoint_updated_at=time.time(),
        )
        self._write()

    def _write(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        with temporary.open("w", encoding="utf-8") as output:
            json.dump(self.document, output, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, self.path)


def availability_sql(changed_ids: str) -> str:
    return """
WITH changed AS MATERIALIZED (""" + changed_ids + """)
 , eligible AS MATERIALIZED (SELECT t.* FROM live_rag.truth t JOIN changed USING (id))
 , application AS MATERIALIZED (
     SELECT c.*, t.id IS NOT NULL AS current FROM live_rag.application c JOIN changed USING (id)
     LEFT JOIN eligible t ON (c.id, c.tenant_id, c.body, c.incarnation, c.revision,
       c.chunk_version, c.model_version, c.embedding) =
       (t.id, t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding))
 , periodic_full AS MATERIALIZED (
     SELECT c.*, t.id IS NOT NULL AS current FROM live_rag.periodic_full c JOIN changed USING (id)
     LEFT JOIN eligible t ON (c.id, c.tenant_id, c.body, c.incarnation, c.revision,
       c.chunk_version, c.model_version, c.embedding) =
       (t.id, t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding))
 , differential AS MATERIALIZED (
     SELECT c.*, t.id IS NOT NULL AS current FROM live_rag.differential c JOIN changed USING (id)
     LEFT JOIN eligible t ON (c.id, c.tenant_id, c.body, c.incarnation, c.revision,
       c.chunk_version, c.model_version, c.embedding) =
       (t.id, t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding))
SELECT json_build_object(
 'eligible', (SELECT count(*) FROM eligible),
 'application', (SELECT count(*) FROM application WHERE current),
 'application_absent', (SELECT count(*) FROM eligible) - (SELECT count(*) FROM application WHERE current),
 'application_stale', (SELECT count(*) FROM application WHERE NOT current),
 'periodic_full', (SELECT count(*) FROM periodic_full WHERE current),
 'periodic_full_absent', (SELECT count(*) FROM eligible) - (SELECT count(*) FROM periodic_full WHERE current),
 'periodic_full_stale', (SELECT count(*) FROM periodic_full WHERE NOT current),
 'differential', (SELECT count(*) FROM differential WHERE current),
 'differential_absent', (SELECT count(*) FROM eligible) - (SELECT count(*) FROM differential WHERE current),
 'differential_stale', (SELECT count(*) FROM differential WHERE NOT current)
)::text;
"""

# Bound the exact check only; measured maintenance and search keep normal settings.
EXACT = """
BEGIN;
SET LOCAL max_parallel_workers_per_gather = 0;
SET LOCAL work_mem = '16MB';
WITH truth AS MATERIALIZED (
    SELECT id, tenant_id, body, incarnation, revision, chunk_version, model_version, embedding
    FROM live_rag.truth
)
SELECT json_build_object(
  'application', (
    (SELECT count(*) FROM live_rag.application) = (SELECT count(*) FROM truth)
    AND NOT EXISTS (
      SELECT 1 FROM live_rag.application c FULL OUTER JOIN truth t USING (id)
      WHERE c.id IS NULL OR t.id IS NULL
         OR ROW(c.tenant_id, c.body, c.incarnation, c.revision, c.chunk_version, c.model_version, c.embedding)
            IS DISTINCT FROM
            ROW(t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding)
    )
  ),
  'periodic_full', (
    (SELECT count(*) FROM live_rag.periodic_full) = (SELECT count(*) FROM truth)
    AND NOT EXISTS (
      SELECT 1 FROM live_rag.periodic_full c FULL OUTER JOIN truth t USING (id)
      WHERE c.id IS NULL OR t.id IS NULL
         OR ROW(c.tenant_id, c.body, c.incarnation, c.revision, c.chunk_version, c.model_version, c.embedding)
            IS DISTINCT FROM
            ROW(t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding)
    )
  ),
  'differential', (
    (SELECT count(*) FROM live_rag.differential) = (SELECT count(*) FROM truth)
    AND NOT EXISTS (
      SELECT 1 FROM live_rag.differential c FULL OUTER JOIN truth t USING (id)
      WHERE c.id IS NULL OR t.id IS NULL
         OR ROW(c.tenant_id, c.body, c.incarnation, c.revision, c.chunk_version, c.model_version, c.embedding)
            IS DISTINCT FROM
            ROW(t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding)
    )
  )
)::text;
COMMIT;
"""


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summary(values: list[float]) -> dict:
    return {"samples": len(values), "p50": percentile(values, .50),
            "p95": percentile(values, .95), "p99": percentile(values, .99)}


def plan_execution_ms(measurement: dict, name: str) -> float:
    return measurement["plans"][name][0]["Execution Time"]


def timed(sql: str, **variables: str) -> tuple[str, float]:
    start = time.monotonic()
    result = worker.psql(sql, **variables)
    return result, time.monotonic() - start


def json_result(output: str) -> dict:
    return json.loads(output.splitlines()[-1])


def database_metadata() -> dict:
    worker.psql("CREATE EXTENSION IF NOT EXISTS vector")
    return json_result(worker.psql("""
SELECT json_build_object('postgres', current_setting('server_version'),
 'pgvector', (SELECT extversion FROM pg_extension WHERE extname='vector'),
 'pg_trickle', (SELECT extversion FROM pg_extension WHERE extname='pg_trickle'),
 'occupied', EXISTS (SELECT 1 FROM pg_namespace WHERE nspname IN ('live_rag', 'embedding_demo')))::text;
"""))


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


def _run(args: argparse.Namespace, checkpoint: CheckpointWriter) -> dict:
    checkpoint.update({"phase": "metadata"})
    metadata = database_metadata()
    if metadata["occupied"]:
        raise ValueError("live_rag or embedding_demo schema already exists; use an empty disposable database")
    if metadata["pg_trickle"] != "0.109.0" or metadata["pgvector"] not in ("0.8.1", "0.8.7") or not re.match(r"^18(?:\.|$)", metadata["postgres"]):
        raise ValueError(
            "expected pg_trickle 0.109.0, PostgreSQL 18 and pgvector 0.8.1 or 0.8.7; "
            f"got pg_trickle={metadata['pg_trickle']!r}, pgvector={metadata['pgvector']!r}, "
            f"PostgreSQL={metadata['postgres']!r}"
        )

    def attest():
        identity = json_result(worker.psql(attestation.IDENTITY_SQL))
        return attestation.attest(args.attest_container, args.candidate_archive,
                                  args.package_sha256, args.candidate_commit, identity)

    initial_attestation = attest() if args.attest_container else None
    base_report = {
        "metadata": metadata,
        "candidate_commit": args.candidate_commit,
        "package_sha256": args.package_sha256,
        "identity_source": "archive manifest and installed files checked before and after run" if initial_attestation else "operator-declared; installed payload not attested",
        "attestation": initial_attestation,
        "rows": args.rows,
        "dimensions": args.dimensions,
        "batch": args.batch,
        "cycles": args.cycles,
        "workload": args.workload,
        "delay_profile": args.delay_profile,
        "mean_delay_seconds_per_job": args.delay_seconds,
        "delay_seed": args.delay_seed,
        "concurrent_readers": args.concurrent_readers,
        "retrieval_tenant": args.tenant,
    }
    samples = []
    checkpoint.update(
        {"phase": "initial_setup", "step": "create_schemas"},
        **base_report,
        samples=samples,
    )

    start = time.monotonic()
    worker.psql(setup_live_rag.setup_sql(args.dimensions))
    checkpoint.update({"phase": "initial_setup", "step": "pause_scheduler"})
    worker.psql(f"SET pg_trickle.scheduler_drain_timeout=3600; "
                f"SELECT pgtrickle.pause_scheduler({SCHEDULER_STREAMS});")

    def resume_scheduler() -> None:
        worker.psql(f"SELECT pgtrickle.resume_scheduler({SCHEDULER_STREAMS});")

    atexit.register(resume_scheduler)
    checkpoint.update({"phase": "initial_setup", "step": "seed"})
    worker.psql(SEED, rows=str(args.rows), dimensions=str(args.dimensions))
    variables = {"rows": str(args.rows), "batch": str(args.batch), "cycle": "0",
                 "dimensions": str(args.dimensions)}
    checkpoint.update({"phase": "initial_maintenance", "state": "started"})
    worker.psql("SELECT live_rag.sync_application(ARRAY(SELECT id FROM live_rag.truth)::bigint[]); "
                "SELECT live_rag.recompute_full(:'dimensions'::integer); "
                "SELECT pgtrickle.refresh_stream_table('live_rag.differential');", **variables)
    checkpoint.update({"phase": "initial_maintenance", "state": "complete"})
    checkpoint.update({"phase": "initial_setup", "step": "create_indexes"})
    worker.psql(setup_live_rag.index_sql(args.dimensions))
    checkpoint.update({"phase": "initial_setup", "step": "analyze"})
    worker.psql("ANALYZE embedding_demo.documents; ANALYZE embedding_demo.results; ANALYZE live_rag.access; "
                "ANALYZE live_rag.application; ANALYZE live_rag.periodic_full; ANALYZE live_rag.differential;")
    initial_seconds = time.monotonic() - start
    checkpoint.update(
        {"phase": "initial_validation", "state": "started"},
        initial_setup_seconds=initial_seconds,
    )
    initial_exact = json_result(worker.psql(EXACT))
    checkpoint.update(
        {"phase": "initial_validation", "state": "complete"},
        initial_corpora_exact=initial_exact,
    )
    if not all(initial_exact.values()):
        raise ValueError(f"initial corpora differ from truth: {initial_exact}")

    checkpoint.update(
        {"phase": "ready", "step": "cycles"},
        **base_report,
        initial_setup_seconds=initial_seconds,
        initial_corpora_exact=initial_exact,
        samples=samples,
    )
    changed_ids = MIXED_IDS if args.workload == "mixed" else CHANGED_IDS
    change = MIXED_CHANGE if args.workload == "mixed" else CHANGE
    availability_query = availability_sql(changed_ids)
    for cycle in range(args.cycles):
        variables["cycle"] = str(cycle)
        names = list(MAINTENANCE)
        names = names[cycle % len(names):] + names[:cycle % len(names)]
        sample = {"cycle": cycle, "order": names, "maintenance": {}, "retrieval": {}}
        samples.append(sample)
        checkpoint.update({"phase": "cycle", "cycle": cycle, "step": "source_change"}, samples=samples)
        start = time.monotonic()
        worker.psql(change, **variables)
        committed = time.monotonic()
        sample["source_transaction_seconds"] = committed - start
        after_commit = json_result(worker.psql(availability_query, **variables))
        sample["availability_after_commit"] = after_commit
        checkpoint.update({"phase": "cycle", "cycle": cycle, "step": "embedding_generation"}, samples=samples)
        generation_start = time.monotonic()
        generation = worker.drain("model-v1", 3600, args.delay_seconds, args.batch, False,
                                  args.dimensions, args.delay_profile, args.delay_seed + cycle)
        eligible = time.monotonic()
        sample.update(
            commit_to_eligible_seconds=eligible - committed,
            generation_seconds=eligible - generation_start,
            generation=generation,
        )
        checkpoint.update(
            {"phase": "cycle", "cycle": cycle, "step": "embedding_generation_complete"},
            samples=samples,
        )
        expected_jobs = (args.batch + 3) // 4 + args.batch // 4 if args.workload == "mixed" else args.batch
        if generation["accepted"] != expected_jobs or generation["stale"]:
            raise ValueError(f"unexpected embedding generation result: {generation}")
        after_generation = json_result(worker.psql(availability_query, **variables))
        sample["availability_after_generation"] = after_generation
        maintenance = sample["maintenance"]
        for name in names:
            sql = "CREATE TEMP TABLE bench_wal AS SELECT pg_current_wal_insert_lsn() AS start_lsn, clock_timestamp() AS started;\n" + MAINTENANCE[name].replace("{changed_ids}", changed_ids) + "\nSELECT json_build_object('cluster_wal_bytes', pg_wal_lsn_diff(pg_current_wal_insert_lsn(), start_lsn), 'server_started', extract(epoch FROM started), 'server_finished', extract(epoch FROM clock_timestamp()))::text FROM bench_wal;"
            progress = {"phase": "maintenance", "cycle": cycle, "approach": name, "state": "started"}
            checkpoint.update(progress, samples=samples)
            if args.concurrent_readers:
                reader_variables = {"corpus": f"{name}_current", "tenant": str(args.tenant),
                                    "query_vector": worker.embedding("seed document 1", "model-v1", args.dimensions),
                                    "terms": "seed"}
                (output, seconds), reads, client_window = live_rag_load.measure_during(
                    lambda: timed(sql, **variables), reader_sql(args.dimensions), reader_variables, args.concurrent_readers)
            else:
                output, seconds = timed(sql, **variables)
                reads = []
            searchable_at = client_window[1] if args.concurrent_readers else time.monotonic()
            maintenance[name] = {"duration_seconds": seconds, "eligible_to_searchable_seconds": searchable_at - eligible,
                                 **json_result(output)}
            if args.concurrent_readers:
                begin, end = maintenance[name]["server_started"], maintenance[name]["server_finished"]
                maintenance[name]["reader_load"] = reader_summary(reads, client_window, begin, end, args.concurrent_readers)
            checkpoint.update(
                {"phase": "maintenance", "cycle": cycle, "approach": name, "state": "measured"},
                samples=samples,
            )
            maintenance[name]["availability"] = json_result(worker.psql(availability_query, **variables))
            checkpoint.update({**progress, "state": "complete"}, samples=samples)
        checkpoint.update({"phase": "cycle", "cycle": cycle, "step": "exact_validation", "state": "started"}, samples=samples)
        exact = json_result(worker.psql(EXACT))
        sample["corpora_exact"] = exact
        if not all(exact.values()):
            raise ValueError(f"corpora differ from truth after cycle {cycle}: {exact}")
        retrieval = sample["retrieval"]
        for name in MAINTENANCE:
            corpus = f"{name}_current"
            progress = {"phase": "retrieval", "cycle": cycle, "approach": name, "state": "started"}
            checkpoint.update(progress, samples=samples)
            output, seconds = timed(recall.measurement_sql(args.dimensions), corpus=corpus,
                                    tenant=str(args.tenant), query_vector=worker.embedding("seed document 1", "model-v1", args.dimensions),
                                    terms="seed", k="10", iterative_scan="strict_order")
            retrieval[name] = {"measurement_command_seconds": seconds, **recall.parse_output(output, corpus)}
            checkpoint.update({**progress, "state": "complete"}, samples=samples)
        checkpoint.update({"phase": "cycle", "cycle": cycle, "step": "differential_refresh_history", "state": "started"}, samples=samples)
        refresh = json_result(worker.psql("""
SELECT row_to_json(h)::text FROM (
 SELECT h.action, h.status, h.was_full_fallback, h.refresh_reason, h.refresh_reason_detail, h.merge_strategy_used
 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id)
 WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1
) h;
"""))
        sample["differential_refresh"] = refresh
        checkpoint.update({"phase": "cycle", "cycle": cycle, "step": "complete"}, samples=samples)

    checkpoint.update({"phase": "final_validation", "step": "relation_sizes"}, samples=samples)
    sizes = json_result(worker.psql("""
SELECT json_build_object('application', pg_total_relation_size('live_rag.application'),
 'periodic_full', pg_total_relation_size('live_rag.periodic_full'),
 'differential', pg_total_relation_size('live_rag.differential'))::text;
"""))
    checkpoint.update(
        {"phase": "final_validation", "step": "attestation"},
        samples=samples,
        relation_bytes_including_indexes=sizes,
    )
    final_attestation = attest() if initial_attestation else None
    if initial_attestation and initial_attestation != final_attestation:
        raise ValueError("candidate or PostgreSQL instance changed during the benchmark")
    resume_scheduler()
    atexit.unregister(resume_scheduler)
    return {**base_report, "initial_setup_seconds": initial_seconds,
            "initial_corpora_exact": initial_exact,
            "samples": samples, "relation_bytes_including_indexes": sizes,
            "batch_latency_seconds": {"source_transaction": summary([s["source_transaction_seconds"] for s in samples]),
              "commit_to_eligible": summary([s["commit_to_eligible_seconds"] for s in samples]),
              "embedding_generation": summary([s["generation_seconds"] for s in samples]),
              "eligible_to_searchable": {name: summary([s["maintenance"][name]["eligible_to_searchable_seconds"] for s in samples]) for name in MAINTENANCE},
              "commit_to_searchable": {name: summary([s["commit_to_eligible_seconds"] + s["maintenance"][name]["eligible_to_searchable_seconds"] for s in samples]) for name in MAINTENANCE},
              **{name: summary([s["maintenance"][name]["duration_seconds"] for s in samples]) for name in MAINTENANCE}},
            "retrieval_latency_ms": {corpus: {plan: summary([
                plan_execution_ms(sample["retrieval"][corpus], plan) for sample in samples
            ]) for plan in ("exact", "planner", "eligibility", "hybrid_exact", "hybrid_planner")}
                for corpus in MAINTENANCE},
            "notes": "Deterministic hash-derived synthetic initial backfill bypasses claims; changed revisions use the fake worker. Uniform delay samples are deterministic per cycle from the recorded seed and have mean delay_seconds_per_job. Timings include psql startup; commit-to-eligible also includes the post-commit availability check. Approaches run serially in rotating order; eligible-to-searchable includes the post-generation availability check. WAL is cluster-wide insert-LSN movement around maintenance; other activity can contaminate it. Retrieval and optional concurrent reads use administrative current views, not restricted-role latency. Reader overlap uses database timestamps and client latencies include psql startup. Concurrent writers and host CPU require the separate load test and Docker stats sampling."}


def run(args: argparse.Namespace) -> dict:
    checkpoint = CheckpointWriter(args.checkpoint)
    try:
        report = _run(args, checkpoint)
        checkpoint.complete(report)
        return report
    except Exception as error:
        try:
            checkpoint.fail(error)
        except OSError:
            pass
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--dimensions", type=int, choices=(384, 768, 1536), required=True)
    parser.add_argument("--batch", type=int, default=10)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=0)
    parser.add_argument("--delay-profile", choices=("fixed", "uniform"), default="fixed")
    parser.add_argument("--delay-seed", type=int, default=0)
    parser.add_argument("--tenant", type=int, default=1)
    parser.add_argument("--workload", choices=("updates", "mixed"), default="updates")
    parser.add_argument("--concurrent-readers", type=int, default=0, help="bounded hybrid readers per maintenance step (0-4)")
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--package-sha256", required=True)
    parser.add_argument("--candidate-archive", help="candidate release archive for Docker payload attestation")
    parser.add_argument("--attest-container", help="Docker container running the benchmark database")
    parser.add_argument("--checkpoint", type=Path, help="atomically save partial and final JSON progress")
    args = parser.parse_args()
    if bool(args.candidate_archive) != bool(args.attest_container):
        parser.error("--candidate-archive and --attest-container must be supplied together")
    if not 1 <= args.rows <= 100_000:
        parser.error("--rows must be between 1 and 100000; larger RAG corpora are out of scope")
    if not 1 <= args.batch <= min(args.rows, 1000) or not 1 <= args.cycles <= 1000 or not 0 <= args.delay_seconds <= 10 or not math.isfinite(args.delay_seconds) or not 1 <= args.tenant <= 20 or not 0 <= args.concurrent_readers <= 4:
        parser.error("batch, cycles, delay, tenant or readers is outside the supported range")
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
