# Live RAG visibility writes during maintenance

On 2026-11-10, the multi-session test ran on PostgreSQL 18.6 in Linux ARM64
Docker on an Apple M1 Pro using the earlier v0.109.0 candidate
`1e291536bf0380b37a509b730155ca3d3a0ccea4`. The image tags identified
pgvector 0.8.1 and 0.8.7; the package was the same earlier archive described
in the [bounded-reader run](../2026-11-10-rag-768-bounded-reader-arm64/README.md).
No separate installed-file attestation was collected for these tests.

For application upsert, periodic full recomputation and differential refresh,
the test held an exclusive lock on that corpus, observed maintenance blocked
on the lock, committed a visibility change to another document, and released
the lock. Guarded current views exposed neither hidden document, and all three
corpora matched the defining query after a subsequent reconciliation. Both
[0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log) passed. The test is
required by both v0.109.0 pgvector release suites.

This uses three documents and one visibility write per maintenance operation.
It proves overlap with a blocked maintenance statement and eventual
convergence, not sustained concurrent throughput, active-scan interleavings,
or final-candidate qualification.
