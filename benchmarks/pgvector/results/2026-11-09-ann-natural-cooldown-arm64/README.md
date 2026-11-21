# Supervisor retry after a natural worker cooldown

On 2026-11-09, the opt-in recovery test ran the packaged supervisor and worker
against the v0.109.0 ARM64 archive from [qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The test source builds on `ae12942a`.

The test runs the supervisor as a restricted maintenance role, blocks a
concurrent HNSW rebuild, and kills its active `psql` worker. Once the backend
exits, it records the request's persisted eligibility deadline and releases
the blocker. It does not change that deadline. Polling observes the request
in `INTERRUPTED` before the deadline; the same supervisor eventually completes
the retry, with completion no earlier than the recorded deadline. The test
checks index validity and retains the surrounding recovery and authorization
checks. Polling samples state rather than recording every claim transition.
An earlier recovery step in the same test advances a different request's
cooldown to exercise revoked privileges; the worker-death deadline is natural.

Both [pgvector 0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log) passed
on PostgreSQL 18.6 / Linux ARM64 Docker on an Apple M1 Pro. The entire tests
took 68.10 and 68.89 seconds, respectively. The [0.8.1](attestation-081.json.gz)
and [0.8.7](attestation-087.json.gz) observations each matched all 157
candidate PostgreSQL payload files, with digest
`0cf46931a74ff096067c1000fddcf70d78362b2f4a25b9f9f3a7aaad39f84001`.
Both scripts came from the extracted archive. The attestation metadata omits
the commit and archive digest; those identities are from the archive and
qualification run above.

This checks one worker interruption per pgvector version on an earlier
candidate. It does not qualify the final integrated package or establish
repeated-failure backoff behavior. To repeat with a prepared image and
extracted candidate, set `PGS_E2E_IMAGE`, `PGT_EXTENSION_DIR`, and
`PGT_ANN_OPERATOR_ROOT`, then run:

```sh
cargo test --features pg18 --test e2e_pgvector_tests \
  test_ann_supervisor_waits_for_natural_worker_cooldown -- --ignored --nocapture
```
