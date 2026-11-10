# Live RAG cancellation and reconciliation

On 2026-11-10, the cancellation test passed with pgvector
[0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log), PostgreSQL 18.6 and
Linux ARM64 Docker on an Apple M1 Pro. It used the earlier v0.109.0 candidate
`1e291536bf0380b37a509b730155ca3d3a0ccea4`; no separate installation
attestation was collected for these tests.

For each corpus approach, a test-only row trigger paused maintenance after it
started processing a row. A separate session committed a visibility change,
then cancelled the maintenance backend. The test required PostgreSQL's query
cancellation error, compared the entire target corpus with its pre-operation
snapshot, checked that guarded views did not expose hidden rows, and retried
all three approaches. Their corpora matched the defining SQL afterward, and
the stream reconciliation was verified differential without full fallback.
Both existing concurrent-write cases also passed after the shared test setup
changed ([0.8.1](concurrency-081.log), [0.8.7](concurrency-087.log)).

The logs show 0.052–0.057 seconds from the committed source write to completed
reconciliation in these six cases. This interval includes cancellation,
barrier cleanup, guarded-view checks and serial reconciliation of all three
corpora; it is not a per-document time-to-searchable measurement or an SLA.
The test uses three documents and an artificial trigger barrier. It does not
cover process or database restart, sustained load, or the final integrated
package. The cancellation case is required by both release suites.
