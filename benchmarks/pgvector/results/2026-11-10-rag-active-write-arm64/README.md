# Live RAG writes during active row maintenance

On 2026-11-10, both [pgvector 0.8.1](pgvector-081.log) and
[0.8.7](pgvector-087.log) passed two multi-session tests on PostgreSQL 18.6
in Linux ARM64 Docker on an Apple M1 Pro. The images used the earlier v0.109.0
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`; no separate installation
attestation was collected for these runs.

The first test blocks each maintenance statement at the corpus lock. The
second installs a test-only row trigger on each target and blocks that trigger
on an advisory lock, establishing that maintenance has started processing a
row before a second session commits a visibility change. It releases the
barrier, checks that guarded views hide both changed documents, then reconciles
and compares all three corpora with the defining SQL. For the stream table,
both tests require a completed differential refresh without full fallback.
Both cases are required in each v0.109.0 pgvector release suite.

The data set has three documents and the active-row case adds a user trigger,
which can affect the chosen merge path. It does not measure sustained writer
load, production performance or the final integrated release package.
