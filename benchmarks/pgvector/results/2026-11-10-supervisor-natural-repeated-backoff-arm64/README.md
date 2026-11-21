# Natural repeated-failure backoff

On 2026-11-10, the packaged supervisor and worker ran as a restricted role on
PostgreSQL 18.6 / Linux ARM64 Docker on an Apple M1 Pro. The test induced two
HNSW concurrent-reindex lock timeouts. It sampled the first failed request
until its approximately 30-second eligibility deadline and observed the
second failure only after that deadline. It did not advance either deadline.
After the second failure, the configured two-failure limit held beyond the
approximately 60-second deadline while a different index's request completed;
the committed source and refreshed stream matched.

The [pgvector 0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log) runs
passed in 98.78 and 99.39 seconds, respectively. Logged lock timeouts are
intentional. They used the earlier v0.109.0 candidate
`1e291536bf0380b37a509b730155ca3d3a0ccea4` and extracted supervisor and
worker, as in the [natural worker-cooldown exercise](../2026-11-09-ann-natural-cooldown-arm64/README.md).
No separate installation attestation was collected for these two runs.

This is one small-corpus run per version; it samples rather than recording
every intermediate claim and does not qualify the final integrated package
or repeated failures under sustained load. The natural-wait test is opt-in;
the faster deadline-advancing variant is required by the release contract.
