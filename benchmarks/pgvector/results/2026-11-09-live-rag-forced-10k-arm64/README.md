# Forced-path recall on a 10,000-document live RAG fixture

This exploratory run used PostgreSQL 18.6, pgvector 0.8.7 and the earlier
v0.109.0 ARM64 archive at commit `1e291536bf0380b37a509b730155ca3d3a0ccea4`
(archive SHA-256 `2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`).
The [installation observation](installation.json.gz) matched all 157 files.
The container image was `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`,
limited to four CPUs, 5 GiB memory and 1 GiB shared memory on an Apple M1 Pro.
The measurement code was a working-tree change based on `bd987fd8`; the SQL
and Python measurement diff SHA-256 was
`d7cc02a0ba0d5333602abf86fc0fc45c0d93639ea708dd0422ddc925b5779b3f`.
These are not final integrated release artifacts.

The fixture used 384 dimensions, 10,000 seeded documents, one mixed batch of
12 slots, deterministic fake embeddings and no artificial delay or controlled
concurrent load. All three corpora matched their defining SQL before and after
the batch. Initial differential population fell back to FULL; the batch used
DIFFERENTIAL with `explicit_dml_reconciled`. The initial setup took 95.7 s.
The three serial maintenance samples were 0.078 s application, 30.492 s FULL
and 0.667 s differential. One sample cannot support a latency distribution or
a general performance claim. Tables were ANALYZEd; no explicit VACUUM was run.

After the batch, the guarded differential view contained 9,997 rows: tenant 1
had 7,996 and tenant 2 had 106. For each tenant and iterative-scan setting,
the [matrix](matrix.json.gz) compares top-10 results to an exact scan using
the same tenant filter and cosine metric. Each cell is one query; the boundary
distance had one row in these samples. The planner chose no ANN index in any
normal query, and all normal-path recall values were 1.0 with no underfill.
With sequential scans discouraged, the differential query used its HNSW index:

| Tenant | `hnsw.iterative_scan` | Forced-path recall@10 | Underfill |
| --- | --- | ---: | ---: |
| 1 | off | 0.2 | 0 |
| 1 | strict_order | 0.2 | 0 |
| 2 | off | 0.0 | 10 |
| 2 | strict_order | 0.8 | 0 |

The forced plans for the application and periodic FULL views did not use their
ANN indexes; their forced-path recall was 1.0 with no underfill, but those are
not ANN-quality measurements. Plans and per-query metrics are in the matrix;
the [batch report](batch.json.gz) includes the initial corpus checks and the
normal and forced paths. Plans and materialized candidates run separately in
the same repeatable-read transaction. `hnsw.ef_search` was 40,
`hnsw.max_scan_tuples` was 20,000 and `hnsw.scan_mem_multiplier` was 1.
The results are one synthetic query after one batch, not a recall guarantee,
latency distribution, or evidence of production planner behavior.

The run used the documented `run_live_rag_batches.py` command with
`--rows 10000 --dimensions 384 --batch 12 --cycles 1 --tenant 2 --workload mixed`,
followed by [forced-recall-matrix.py](forced-recall-matrix.py) against the same
disposable database. The [batch log](forced-recall-10k.log),
[matrix log](forced-recall-matrix.log), and package-backed E2E test logs for
[0.8.1](forced-recall-081.log) and [0.8.7](forced-recall-087.log) are retained.
The uncompressed batch and matrix JSON SHA-256 values are respectively
`8b74b12848723bac159e6cb1d6a21ea74718f078278e271af26e105f27b21d44` and
`4b9ae96521f02f56b0891c4bd7e3c72cb4ce31a2eba18c63dc3ee5b3ff7a2ce3`.
