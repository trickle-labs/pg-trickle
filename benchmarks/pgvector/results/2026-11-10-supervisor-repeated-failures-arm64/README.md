# Supervisor repeated failures and retry limit

On 2026-11-10, the supervisor ran as a restricted maintenance role against
PostgreSQL 18.6 on Linux ARM64 Docker on an Apple M1 Pro. A held table lock
forced two concurrent-reindex lock timeouts on one HNSW index. The test
observed persisted failure counts and approximately 30- and 60-second
eligibility deadlines. It advanced both deadlines rather than waiting for
them. After the configured two-failure limit, another index's request
completed while the failed request stayed failed, even with its deadline
advanced. A committed source change and refreshed corpus remained equal.

The [0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log) runs passed;
the logged lock-timeout errors are intentional. The containers used the
earlier v0.109.0 candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4` and
the supervisor and worker from its extracted archive, as in the
[natural-cooldown exercise](../2026-11-09-ann-natural-cooldown-arm64/README.md).
No separate installation attestation was collected for these two runs.

This checks two consecutive failures on a small corpus. It does not measure
natural elapsed backoff for both failures, repeated faults under sustained
load, or the final integrated candidate. The test is required for both
pgvector versions by the release contract.
