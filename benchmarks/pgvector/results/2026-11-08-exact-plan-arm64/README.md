# Exact-baseline plan smoke check

On 2026-11-08 the packaged PostgreSQL 18.6 / pgvector 0.8.7 image
`sha256:29fff085c0b9f56af7a9c7cd16af4c0842fa3427614ef934132aa14d7b5e763d`
ran on an Apple M1 Pro using Linux arm64 containers. The image was built from
the source subsequently committed as `0dcb4a94`; it has no source revision
label. The benchmark harness was based on `cd6c2e47` with the exact-plan
recording change applied.

The run used 1,000 deterministic 384-dimensional vectors, two retrieval samples
per index method, five one-row centroid rounds and no controlled host load.
It asserted that the HNSW and IVFFlat plans used the requested ANN index and
that each exact baseline scanned the corpus sequentially without that index.
The full observations and plans are in [report.json](report.json). Two samples
are insufficient for performance or recall conclusions; this run validates
the measurement path only.
