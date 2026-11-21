# Restricted supervisor during concurrent refresh

On 2026-11-15, test source `bf385baad8e6382e923af30a74116f87c53c6ccc`
passed with pgvector [0.8.1](supervisor-081.log) and
[0.8.7](supervisor-087.log) on PostgreSQL 18.6, Linux ARM64 Docker on an
Apple M1 Pro. The v0.109.0 package was from integrated commit
`9a0e39a121001b1729ec9ab082499bd8e432cfdd`, artifact `13734025185` of
[qualification run 42734523338](https://github.com/trickle-labs/pg-trickle/actions/runs/42734523338),
archive SHA-256 `27c3421626d83fa83fc0b427a39e5dc6674254a1588ce6674bf3c9ba078074db`.
The supervisor and worker scripts came from that extracted archive.

For each HNSW and IVFFlat index, an old snapshot held a concurrent rebuild
open while 120 paced source inserts committed, each followed by a stream
refresh and an indexed nearest-neighbor read. The supervisor connected as a
non-superuser granted `SELECT`, `MAINTAIN`, schema usage, and the required
API functions; the test asserted that role could not read the private request
queue. The writer and reader used the test administrator. It checked request
success and final corpus equality.

HNSW and IVFFlat took 33.01 and 34.73 seconds with 0.8.1, and 33.07 and
32.96 seconds with 0.8.7. These are single, paced runs on a small corpus with
an artificially held snapshot, not a capacity or production-latency claim.
They do not test a restricted reader, sustained failure, or a large corpus.

The package was copied into each container before startup. The
[0.8.1](attest-081.json.gz) and [0.8.7](attest-087.json.gz) records each
match all 157 installed files to the candidate, with payload digest
`1fed301b8ff98ccf9922e91bf48a4579552a84da4ebd6d473e202e1ca0d279f0`.
The observations do not inspect mapped library bytes. Main advanced after
the package was built, so this is not qualification of the updated base.
Formatting and lint passed on the test source before these runs.
