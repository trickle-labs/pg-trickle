# Paced visibility writes and tenant reads during refresh

On 2026-11-10, the test passed on PostgreSQL 18.6 with pgvector
[0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log), in Linux ARM64
Docker on an Apple M1 Pro. It used the earlier v0.109.0 candidate
`1e291536bf0380b37a509b730155ca3d3a0ccea4`; no separate installation
attestation was collected for these runs.

The test seeded 1,000 documents with identical 384-dimensional vectors,
assigned 900 to one tenant and 100 to another, and paused a differential
refresh inside a test-only row trigger. While it was paused, another session
committed 100 visibility changes. A restricted tenant role checked for hidden
or foreign rows and ran a vector-ordered query after each commit. The loop
included a 50 ms pause per iteration and took 6.952 and 6.958 seconds in the
two runs. After releasing the barrier, the test required the refresh and a
subsequent reconciliation to remain differential and compared the entire
stream with its defining SQL. The test is required by both release suites.

These are one small synthetic run per version. Reads follow writes rather than
running simultaneously with them, and the trigger holds the refresh at one
row throughout the loop. Equal vectors, an artificial barrier and deliberate
pacing make the elapsed time unsuitable as a throughput or latency claim. The
test does not qualify the final package or replace a sustained mixed-workload
benchmark with an unblocked refresh.
