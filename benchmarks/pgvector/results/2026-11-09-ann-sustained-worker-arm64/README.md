# Paced reads and refreshes during ANN rebuilds

On 2026-11-09, the opt-in sustained concurrent-rebuild test ran against the
v0.109.0 ARM64 archive from [qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The test source builds on `cfed79c71016ccadea5a969239857f6eb131a1f5`.

For each index type, an old snapshot holds a `REINDEX CONCURRENTLY` request
open while the packaged one-request worker executes. The test commits 120
inserts, refreshes after each, verifies an ANN-index plan and its nearest
neighbor, and paces operations by 250 ms. It checks the worker succeeds and
the completed stream exactly matches its source. The sequence lasted at least
30 seconds per index; it is a controlled, paced workload on a small corpus.

On PostgreSQL 18.6 / Linux ARM64 Docker on an Apple M1 Pro, the
[pgvector 0.8.1 run](pgvector-081.log) took 35.20 seconds for HNSW and
35.09 seconds for IVFFlat. The [0.8.7 run](pgvector-087.log) took 35.13
and 33.84 seconds, respectively. Images were
`sha256:cdff3048cba997d65c23596692fed5133696895473cdd2f53bb6b284f4787e95`
and `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The [0.8.1](attestation-081.json.gz) and [0.8.7](attestation-087.json.gz)
observations each matched all 157 candidate PostgreSQL payload files, with
payload digest `0cf46931a74ff096067c1000fddcf70d78362b2f4a25b9f9f3a7aaad39f84001`.
The worker SQL came from the same extracted archive. The attestation metadata
does not include the commit or archive digest; those identities are from the
archive and qualification run above.

This exercises the one-request worker, not the supervisor under sustained
load. It does not establish throughput capacity, natural cooldown behavior,
or qualification of the final integrated candidate.

To repeat against a prepared pgvector test image and extracted candidate,
set `PGS_E2E_IMAGE`, `PGT_EXTENSION_DIR`, and `PGT_ANN_OPERATOR_ROOT`, then run:

```sh
cargo test --features pg18 --test e2e_pgvector_tests \
  test_ann_worker_sustains_reads_and_refresh_during_concurrent_reindex -- --ignored --nocapture
```
