# Paced writes and reads with the ANN supervisor

On 2026-11-09, an opt-in test ran the packaged supervisor and worker against
the v0.109.0 ARM64 archive from [qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The test source builds on `f8f43c46`.

For each HNSW and IVFFlat index, an old snapshot held the supervisor's
`REINDEX CONCURRENTLY` request open. The test paced 120 committed inserts at
250 ms intervals, refreshing after each and checking an indexed nearest-neighbor
read. It verified the supervisor completed the request, then checked that the
stream matched its source. The supervisor ran as `postgres` for this test;
least-privilege coverage is in separate tests.

On PostgreSQL 18.6 / Linux ARM64 Docker on an Apple M1 Pro, the
[pgvector 0.8.1 run](pgvector-081.log) took 35.14 seconds for HNSW and
34.97 seconds for IVFFlat. The [0.8.7 run](pgvector-087.log) took 34.90
and 34.22 seconds. The [0.8.1](attestation-081.json.gz) and
[0.8.7](attestation-087.json.gz) observations each matched all 157 candidate
PostgreSQL payload files, with digest
`0cf46931a74ff096067c1000fddcf70d78362b2f4a25b9f9f3a7aaad39f84001`.
Both scripts came from the same extracted archive. The attestation metadata
does not include the commit or archive digest; those identities are from the
archive and qualification run above.

This is a paced small-corpus test with an intentionally held snapshot, not a
throughput-capacity claim, natural cooldown test, or qualification of the
final integrated candidate. To repeat with a prepared image and extracted
candidate, set `PGS_E2E_IMAGE`, `PGT_EXTENSION_DIR`, and
`PGT_ANN_OPERATOR_ROOT`, then run:

```sh
cargo test --features pg18 --test e2e_pgvector_tests \
  test_ann_supervisor_sustains_reads_and_refresh_during_concurrent_reindex -- --ignored --nocapture
```
