# Local-source RAG concurrency check

This is functional concurrency evidence, not exact-package qualification. The
E2E image was built locally from RAG PR checkout `74d8c4ecb960d0525062952fa4aae2de0ed4b811`
with `just build-e2e-image`, then combined with the pinned pgvector 0.8.7
image. The test reported pg_trickle 0.109.0, PostgreSQL 18.6 and pgvector
0.8.7. The build was ARM64; no release archive digest was used. Full output is
in `10k-384-run.log` (run ID `full-e2e-97846-1794790507`).

The ignored `test_live_rag_unblocked_writes_and_reads_during_refresh` case
completed 100 paced source writes at 10k documents and 384 dimensions with a
restricted tenant reader and 200 ms maintenance target. Its server-timestamp
overlaps were:

| Approach | Maintenance calls / total time | Writes overlapping maintenance | Guard queries overlapping | Vector reads overlapping |
|---|---:|---:|---:|---:|
| Application batching | 28 / 5.605 s | 3 of 100 | 4 of 89 | 14 of 89 |
| Periodic full | 1 / 30.916 s | 100 of 100 | 0 of 1 | 0 of 1 |
| Differential | 9 / 5.790 s | 99 of 100 | 99 of 100 | 99 of 100 |

All 100 source writes completed during the periodic-full maintenance interval,
but its only guard and vector read were not executed during that interval. The
single guard client call waited about 31 seconds for full maintenance to end.
The differential path allowed almost every paced write, tenant guard and
vector read to overlap maintenance. This small synthetic case shows functional
progress under load; it does not establish a capacity limit or production
latency advantage. The standalone ANN benchmark and exact-package release
qualification remain separate.

To reproduce, build both images for the same Docker platform and run:

```sh
PGS_E2E_PLATFORM=linux/arm64 PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:0.8.7 \
PGVECTOR_EXPECTED_VERSION=0.8.7 PGT_RAG_LOAD_ROWS=10000 \
PGT_RAG_LOAD_DIMENSIONS=384 PGT_RAG_LOAD_CHANGES=100 \
  ./scripts/run_e2e_tests.sh --retries 0 --test e2e_pgvector_revision_tests \
    --run-ignored ignored-only --no-capture \
    -E 'test(test_live_rag_unblocked_writes_and_reads_during_refresh)'
```
