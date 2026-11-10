# Live RAG recovery after PostgreSQL crash

On 2026-11-10, the crash and cancellation tests passed with pgvector
[0.8.1](recovery-081.log) and [0.8.7](recovery-087.log), PostgreSQL 18.6 and
Linux ARM64 Docker on an Apple M1 Pro. They used the earlier v0.109.0 candidate
`1e291536bf0380b37a509b730155ca3d3a0ccea4`; no separate installation
attestation was collected for these tests.

For application, periodic-full and differential maintenance in turn, a
test-only row trigger paused the operation after it began processing a row.
A second session committed a visibility change, and Docker sent SIGKILL to
the dedicated PostgreSQL container. After restart, the test checked both
committed visibility changes, compared the interrupted target corpus with its
pre-operation snapshot, verified guarded views, and reconciled all three
corpora against the defining SQL. Stream reconciliation was required to use a
completed differential refresh without full fallback. The cancellation case
and the two existing concurrent-write cases also passed after the shared test
setup changed ([0.8.1](concurrency-081.log), [0.8.7](concurrency-087.log)).

The six crash cases took 1.379–1.428 seconds from the committed source change
to completed reconciliation. This includes the kill, restart, reconnect,
checks and serial reconciliation of all three corpora; it is not a
per-document time-to-searchable measure or an SLA. The test uses three
documents and an artificial trigger barrier. It does not cover power loss,
sustained load or the final integrated package. The crash case is required in
both pgvector release suites.
