#!/usr/bin/env python3
"""Measure the capped live RAG workload against the exact release package."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import run_live_rag_batches


ROOT = Path(__file__).resolve().parents[2]
ROWS = (10_000, 100_000)
DIMENSIONS = (384, 768, 1536)
BATCH = 1000
CYCLES = 3
DELAY_SECONDS = 0.02
DELAY_SEED = 1396
TENANT = 2
POSTGRES_PASSWORD = "release-qualification"
APPROACHES = ("application", "differential", "periodic_full")
APPROACH_LABELS = {
    "application": "Application",
    "differential": "Differential",
    "periodic_full": "Periodic full",
}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--rows", type=int, choices=ROWS, nargs="+", required=True)
    result.add_argument("--dimensions", type=int, choices=DIMENSIONS, nargs="+", required=True)
    return result


def docker(*args: str, capture_output: bool = False) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip() if capture_output else ""


class DockerStats:
    def __init__(self, container: str, path: Path):
        self.container = container
        self.path = path
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")
        self.thread.start()

    def stop(self) -> None:
        self.stopped.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=20)

    def _sample(self) -> None:
        with self.path.open("a", encoding="utf-8") as output:
            while not self.stopped.is_set():
                result = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{json .}}", self.container],
                    check=False,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                sample = {
                    "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
                    "stats": result.stdout.strip() if result.returncode == 0 else None,
                    "error": result.stderr.strip() if result.returncode != 0 else None,
                }
                output.write(json.dumps(sample, sort_keys=True) + "\n")
                output.flush()
                if self.stopped.wait(15):
                    break


def wait_for_database(container: str) -> None:
    for _ in range(90):
        result = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", "postgres", "-d", "postgres"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            return
        time.sleep(2)
    logs = subprocess.run(["docker", "logs", container], check=False, text=True, capture_output=True)
    raise RuntimeError(f"PostgreSQL did not become ready: {logs.stderr[-2000:]} {logs.stdout[-2000:]}")


def start_database(image: str, container: str) -> int:
    try:
        docker(
            "run",
            "--detach",
            "--name",
            container,
            "--shm-size",
            "2g",
            "--publish",
            "127.0.0.1::5432",
            "--env",
            f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
            image,
        )
        wait_for_database(container)
        port = docker("port", container, "5432/tcp", capture_output=True).rsplit(":", 1)[-1]
        return int(port)
    except Exception:
        subprocess.run(["docker", "rm", "-f", container], check=False, stdout=subprocess.DEVNULL)
        raise


def connection_environment(port: int) -> dict[str, str]:
    return {
        "PGHOST": "127.0.0.1",
        "PGPORT": str(port),
        "PGUSER": "postgres",
        "PGPASSWORD": POSTGRES_PASSWORD,
        "PGDATABASE": "postgres",
    }


def run_case(rows: int, dimensions: int, measurement_path: Path) -> dict:
    candidate_commit = os.environ["PGT_RELEASE_CANDIDATE_COMMIT"]
    package_sha256 = os.environ["PGT_RELEASE_ARTIFACT_SHA256"]
    candidate_archive = Path(os.environ["PGT_RELEASE_CANDIDATE_ARCHIVE"])
    candidate_root = os.environ["PGT_EXTENSION_DIR"]
    attestation_path = os.environ["PGT_RELEASE_ATTESTATION_PATH"]
    image = os.environ["PGS_E2E_IMAGE"]
    container = f"pgt-live-rag-{os.getpid()}-{rows}-{dimensions}"
    port = start_database(image, container)
    size = f"{rows // 1000}k"
    stats_path = measurement_path.with_name(f"live-rag-{size}-{dimensions}-docker-stats.jsonl")
    checkpoint_path = measurement_path.with_name(f"live-rag-{size}-{dimensions}-checkpoint.json")
    stats = DockerStats(container, stats_path)
    connection_vars = connection_environment(port)
    previous = {key: os.environ.get(key) for key in connection_vars}
    os.environ.update(connection_vars)
    try:
        version = subprocess.run(
            ["psql", "-X", "-w", "-qAt", "-c", "SELECT split_part(current_setting('server_version'), ' ', 1)"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
        print(f"PGT_ACTUAL_POSTGRESQL_VERSION={version}", flush=True)
        subprocess.run(
            [
                sys.executable,
                "scripts/release_install_attestation.py",
                "--container",
                container,
                "--candidate-root",
                candidate_root,
                "--output",
                attestation_path,
            ],
            check=True,
            env=os.environ.copy(),
        )
        stats.start()
        args = argparse.Namespace(
            rows=rows,
            dimensions=dimensions,
            batch=BATCH,
            cycles=CYCLES,
            delay_seconds=DELAY_SECONDS,
            delay_profile="uniform",
            delay_seed=DELAY_SEED,
            tenant=TENANT,
            workload="mixed",
            concurrent_readers=1,
            candidate_commit=candidate_commit,
            package_sha256=package_sha256,
            candidate_archive=candidate_archive,
            attest_container=container,
            checkpoint=checkpoint_path,
        )
        report = run_live_rag_batches.run(args)
        report["docker_stats_file"] = stats_path.relative_to(ROOT).as_posix()
        report["checkpoint_file"] = checkpoint_path.relative_to(ROOT).as_posix()
        return report
    finally:
        stats.stop()
        subprocess.run(["docker", "rm", "-f", container], check=False, stdout=subprocess.DEVNULL)
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def write_measurement(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def decision_table(reports: list[dict]) -> dict:
    maintenance_rows = []
    retrieval_rows = []
    for report in reports:
        times = {
            name: report["batch_latency_seconds"][name]["p50"]
            for name in APPROACHES
        }
        exact = all(report["initial_corpora_exact"].values()) and all(
            all(sample["corpora_exact"].values()) for sample in report["samples"]
        )
        differential = times["differential"]
        comparisons = {}
        for baseline in ("application", "periodic_full"):
            value = times[baseline]
            comparisons[baseline] = {
                "outcome": "wins" if differential < value else "loses" if differential > value else "same median",
                "ratio": differential / value,
            }
        maintenance_rows.append({
            "rows": report["rows"],
            "dimensions": report["dimensions"],
            "cycles": report["cycles"],
            "exact_corpus": exact,
            "p50_seconds": times,
            "differential_comparison": comparisons,
        })

        for name in APPROACHES:
            measurements = [sample["retrieval"][name] for sample in report["samples"]]
            metrics = [item["metrics"] for item in measurements]
            forced_samples = [item for item in measurements if item["forced_plan_used_ann_index"]]
            forced = [
                item["metrics"]["forced_recall_at_k"]
                for item in forced_samples
                if item["metrics"].get("forced_recall_at_k") is not None
            ]
            retrieval_rows.append({
                "rows": report["rows"],
                "dimensions": report["dimensions"],
                "corpus": name,
                "samples": len(metrics),
                "planner_hnsw_samples": sum(item["planner_used_ann_index"] for item in measurements),
                "forced_hnsw_samples": len(forced_samples),
                "planner_recall_at_10_p50": statistics.median(
                    metric["recall_at_k"] for metric in metrics
                ),
                "forced_ann_recall_at_10_p50": statistics.median(forced) if forced else None,
                "hybrid_recall_at_10_p50": statistics.median(
                    metric["hybrid_recall_at_10"] for metric in metrics
                ),
                "hybrid_underfill_p50": statistics.median(
                    metric["hybrid_underfill"] for metric in metrics
                ),
            })
    return {"maintenance": maintenance_rows, "retrieval": retrieval_rows}


def write_decision_table(path: Path, report: dict, status: str) -> None:
    rows = report["decision_table"]
    lines = [
        "# Exact-package live RAG decision table",
        "",
        f"Status: {status}; completed cases: {len(rows['maintenance'])}/6.",
    ]
    if report["cases"]:
        candidate = report["cases"][0]
        metadata = candidate["metadata"]
        lines.extend([
            f"Candidate: `{candidate['candidate_commit']}`; package SHA-256 `{candidate['package_sha256']}`.",
            f"Environment: PostgreSQL {metadata['postgres']}; pgvector {metadata['pgvector']}; pg_trickle {metadata['pg_trickle']}.",
        ])
    lines.extend([
        "",
        "Maintenance uses three serial mixed-update cycles per case and compares p50 seconds. "
        "`wins` and `loses` are relative to the named baseline for maintenance time only; "
        "they do not imply a retrieval or end-to-end advantage.",
        "",
        "| Rows / dimensions | Exact corpus | Application p50 (s) | Differential p50 (s) | Periodic full p50 (s) | Differential vs application | Differential vs periodic full |",
        "|---:|:---:|---:|---:|---:|:---|:---|",
    ])
    for row in rows["maintenance"]:
        times = row["p50_seconds"]
        app = row["differential_comparison"]["application"]
        full = row["differential_comparison"]["periodic_full"]
        lines.append(
            f"| {row['rows']:,} / {row['dimensions']} | {'pass' if row['exact_corpus'] else 'FAIL'} "
            f"| {times['application']:.3f} | {times['differential']:.3f} | {times['periodic_full']:.3f} "
            f"| {app['outcome']} ({app['ratio']:.2f}x) | {full['outcome']} ({full['ratio']:.2f}x) |"
        )
    if not rows["maintenance"]:
        lines.append("| No completed cases | pending | - | - | - | - | - |")
    lines.extend([
        "",
        "Retrieval is separate from exact corpus correctness. Planner and forced-plan HNSW use are "
        "counted independently; forced recall is reported only for samples whose forced plan used HNSW.",
        "Retrieval uses tenant 2 and the deterministic seed-document query vector, one query vector per case.",
        "",
        "| Rows / dimensions | Corpus | Planner HNSW samples | Forced HNSW samples | Planner recall@10 p50 | Forced ANN recall@10 p50 | Hybrid recall@10 p50 | Hybrid underfill p50 |",
        "|---:|:---|---:|---:|---:|---:|---:|---:|",
    ])
    for row in rows["retrieval"]:
        if row["forced_hnsw_samples"] == 0:
            forced = "n/a (no HNSW)"
        elif row["forced_ann_recall_at_10_p50"] is None:
            forced = "n/a (recall missing)"
        else:
            forced = f"{row['forced_ann_recall_at_10_p50']:.3f}"
        lines.append(
            f"| {row['rows']:,} / {row['dimensions']} | {APPROACH_LABELS[row['corpus']]} "
            f"| {row['planner_hnsw_samples']}/{row['samples']} | {row['forced_hnsw_samples']}/{row['samples']} "
            f"| {row['planner_recall_at_10_p50']:.3f} | {forced} "
            f"| {row['hybrid_recall_at_10_p50']:.3f} | {row['hybrid_underfill_p50']:.1f} |"
        )
    if not rows["retrieval"]:
        lines.append("| No completed cases | - | - | - | - | - | - | - |")
    lines.extend([
        "",
        "The six synthetic cases are capped at 100,000 documents. The results do not validate larger RAG corpora, "
        "concurrent-writer throughput, host CPU or lock behavior, or a universal speedup. Inspect the retained JSON "
        "for p95/p99 latency, query plans, WAL, relation sizes, Docker CPU/memory samples and per-cycle outcomes.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def write_maintenance_plot(path: Path, rows: list[dict]) -> None:
    width = 1000
    height = max(260, 84 * len(rows) + 110)
    left, top = 210, 55
    plot_width = width - left - 100
    values = [value for row in rows for value in row["p50_seconds"].values() if value > 0]
    low = min(0.1, min(values)) if values else 0.1
    high = max(1.0, max(values)) if values else 1.0
    low_log, high_log = math.log10(low), math.log10(high)
    if high_log == low_log:
        high_log += 1
    colors = {"application": "#4e79a7", "differential": "#59a14f", "periodic_full": "#f28e2b"}
    chunks = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="Median maintenance latency by corpus implementation">',
              '<rect width="100%" height="100%" fill="white"/>',
              '<text x="30" y="30" font-family="sans-serif" font-size="19">Median maintenance latency (p50 seconds, log scale)</text>']
    for exponent in range(math.floor(low_log), math.ceil(high_log) + 1):
        tick = 10 ** exponent
        if not low <= tick <= high:
            continue
        x = left + (exponent - low_log) / (high_log - low_log) * plot_width
        chunks.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{height-50}" stroke="#ddd"/>')
        chunks.append(f'<text x="{x:.1f}" y="{height-27}" text-anchor="middle" font-family="sans-serif" font-size="12">{tick:g}s</text>')
    for index, row in enumerate(rows):
        group_top = top + index * 84
        chunks.append(f'<text x="{left-12}" y="{group_top+31}" text-anchor="end" font-family="sans-serif" font-size="13">{row["rows"]//1000}k / {row["dimensions"]}d</text>')
        for bar_index, name in enumerate(APPROACHES):
            value = row["p50_seconds"][name]
            x_end = left + (math.log10(max(value, low)) - low_log) / (high_log - low_log) * plot_width
            y = group_top + bar_index * 18
            chunks.append(f'<rect x="{left}" y="{y}" width="{max(1, x_end-left):.1f}" height="13" fill="{colors[name]}"/>')
            chunks.append(f'<text x="{x_end+5:.1f}" y="{y+11}" font-family="sans-serif" font-size="11">{value:.2f}</text>')
    legend_x = left
    for name in APPROACHES:
        chunks.append(f'<rect x="{legend_x}" y="{height-12}" width="11" height="11" fill="{colors[name]}"/>')
        chunks.append(f'<text x="{legend_x+16}" y="{height-3}" font-family="sans-serif" font-size="12">{APPROACH_LABELS[name]}</text>')
        legend_x += 150
    if not rows:
        chunks.append(f'<text x="{left}" y="{top+28}" font-family="sans-serif" font-size="14">No completed cases</text>')
    chunks.append("</svg>\n")
    path.write_text("\n".join(chunks), encoding="utf-8")


def write_retrieval_plot(path: Path, rows: list[dict]) -> None:
    width = 1000
    height = max(260, 84 * len({(row['rows'], row['dimensions']) for row in rows}) + 110)
    left, top = 210, 55
    plot_width = width - left - 100
    colors = {"application": "#4e79a7", "differential": "#59a14f", "periodic_full": "#f28e2b"}
    cases = sorted({(row["rows"], row["dimensions"]) for row in rows})
    chunks = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="Hybrid recall at ten by corpus implementation">',
              '<rect width="100%" height="100%" fill="white"/>',
              '<text x="30" y="30" font-family="sans-serif" font-size="19">Hybrid recall@10 vs same-filter exact candidates</text>']
    for tick in (0, 0.25, 0.5, 0.75, 1):
        x = left + tick * plot_width
        chunks.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{height-50}" stroke="#ddd"/>')
        chunks.append(f'<text x="{x:.1f}" y="{height-27}" text-anchor="middle" font-family="sans-serif" font-size="12">{tick:.2f}</text>')
    for index, case in enumerate(cases):
        case_rows = {row["corpus"]: row for row in rows if (row["rows"], row["dimensions"]) == case}
        group_top = top + index * 84
        chunks.append(f'<text x="{left-12}" y="{group_top+31}" text-anchor="end" font-family="sans-serif" font-size="13">{case[0]//1000}k / {case[1]}d</text>')
        for bar_index, name in enumerate(APPROACHES):
            row = case_rows.get(name)
            if row is None:
                continue
            value = row["hybrid_recall_at_10_p50"]
            y = group_top + bar_index * 18
            chunks.append(f'<rect x="{left}" y="{y}" width="{max(1, value*plot_width):.1f}" height="13" fill="{colors[name]}"/>')
            chunks.append(f'<text x="{left+value*plot_width+5:.1f}" y="{y+11}" font-family="sans-serif" font-size="11">{value:.3f}</text>')
    legend_x = left
    for name in APPROACHES:
        chunks.append(f'<rect x="{legend_x}" y="{height-12}" width="11" height="11" fill="{colors[name]}"/>')
        chunks.append(f'<text x="{legend_x+16}" y="{height-3}" font-family="sans-serif" font-size="12">{APPROACH_LABELS[name]}</text>')
        legend_x += 150
    if not cases:
        chunks.append(f'<text x="{left}" y="{top+28}" font-family="sans-serif" font-size="14">No completed cases</text>')
    chunks.append("</svg>\n")
    path.write_text("\n".join(chunks), encoding="utf-8")


def write_report_artifacts(measurement_path: Path, result: dict, evidence: list[Path]) -> None:
    decisions = decision_table(result["cases"])
    decision_path = measurement_path.with_name("live-rag-100k-decision-table.md")
    maintenance_plot = measurement_path.with_name("live-rag-100k-maintenance-p50.svg")
    retrieval_plot = measurement_path.with_name("live-rag-100k-hybrid-recall.svg")
    result["decision_table"] = decisions
    result["decision_table_file"] = decision_path.relative_to(ROOT).as_posix()
    result["plot_files"] = [maintenance_plot.relative_to(ROOT).as_posix(), retrieval_plot.relative_to(ROOT).as_posix()]
    write_decision_table(decision_path, result, result["status"])
    write_maintenance_plot(maintenance_plot, decisions["maintenance"])
    write_retrieval_plot(retrieval_plot, decisions["retrieval"])
    result["raw_evidence_files"] = sorted(
        path.relative_to(ROOT).as_posix()
        for path in [*evidence, decision_path, maintenance_plot, retrieval_plot]
        if path.is_file()
    )
    write_measurement(measurement_path, result)


def main() -> int:
    args = parser().parse_args()
    if tuple(args.rows) != ROWS or tuple(args.dimensions) != DIMENSIONS:
        parser().error(f"release matrix requires rows {ROWS} and dimensions {DIMENSIONS} in order")
    measurement_path = Path(os.environ["PGS_RELEASE_MEASUREMENT_JSON"]).resolve()
    if not measurement_path.is_relative_to(ROOT):
        parser().error("measurement output must stay inside the repository workspace")
    reports: list[dict] = []
    evidence: list[Path] = []
    for rows in args.rows:
        size = f"{rows // 1000}k"
        for dimensions in args.dimensions:
            evidence.extend(
                [
                    measurement_path.with_name(f"live-rag-{size}-{dimensions}-docker-stats.jsonl"),
                    measurement_path.with_name(f"live-rag-{size}-{dimensions}-checkpoint.json"),
                ]
            )
    for path in evidence:
        path.unlink(missing_ok=True)
    decision_path = measurement_path.with_name("live-rag-100k-decision-table.md")
    maintenance_plot = measurement_path.with_name("live-rag-100k-maintenance-p50.svg")
    retrieval_plot = measurement_path.with_name("live-rag-100k-hybrid-recall.svg")
    for path in (decision_path, maintenance_plot, retrieval_plot):
        path.unlink(missing_ok=True)
    result = {
        "status": "running",
        "rows": list(args.rows),
        "dimensions": list(args.dimensions),
        "cases": reports,
        "raw_evidence_files": [],
        "candidate_commit": os.environ["PGT_RELEASE_CANDIDATE_COMMIT"],
        "package_sha256": os.environ["PGT_RELEASE_ARTIFACT_SHA256"],
    }
    write_report_artifacts(measurement_path, result, evidence)
    try:
        for rows in args.rows:
            for dimensions in args.dimensions:
                print(f"=== Exact-package live RAG: {rows} rows, {dimensions} dimensions ===", flush=True)
                report = run_case(rows, dimensions, measurement_path)
                reports.append(report)
                write_report_artifacts(measurement_path, result, evidence)
        result["status"] = "complete"
        result["workload"] = "10,000/100,000-document mixed update/insert/delete corpus, 384/768/1536 dimensions"
        write_report_artifacts(measurement_path, result, evidence)
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    except Exception as error:
        result["status"] = "failed"
        result["error"] = f"{type(error).__name__}: {error}"
        write_report_artifacts(measurement_path, result, evidence)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
