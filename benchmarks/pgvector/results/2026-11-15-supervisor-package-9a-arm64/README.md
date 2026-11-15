# Packaged supervisor during concurrent refresh

On 2026-11-15 the opt-in supervisor test passed with pgvector
[0.8.1](supervisor-081.log) and [0.8.7](supervisor-087.log), PostgreSQL 18.6,
and Linux ARM64 Docker on an Apple M1 Pro. The test source and v0.109.0
package came from integrated commit `9a0e39a121001b1729ec9ab082499bd8e432cfdd`.
The archive was artifact `13734025185` from
[qualification run 42734523338](https://github.com/trickle-labs/pg-trickle/actions/runs/42734523338),
with SHA-256 `27c3421626d83fa83fc0b427a39e5dc6674254a1588ce6674bf3c9ba078074db`.
The extracted supervisor and worker scripts had SHA-256 values
`9c9137be246dc16fa8985694b6a49375d1b291c37b8f6f6750de189adbd99752` and
`dd9d86ea7d9f2118b0d81f675872514baa686f733bfe873606cdd62a13cad233`.

For each HNSW and IVFFlat index, an old snapshot held the supervisor's
concurrent rebuild open while 120 inserts committed at 250 ms intervals;
each was followed by a stream refresh and indexed nearest-neighbor read.
The test checked completion and final corpus equality. HNSW and IVFFlat took
32.98 and 32.78 seconds with 0.8.1, and 32.87 and 33.37 seconds with 0.8.7.
These are single paced samples on a small corpus, with a superuser and an
artificially held snapshot; they do not establish capacity or least-privilege
behavior under sustained load.

The package was copied into each test container before startup. The
[0.8.1](attest-081.json.gz) and [0.8.7](attest-087.json.gz) observations
each match all 157 candidate files and payload digest
`1fed301b8ff98ccf9922e91bf48a4579552a84da4ebd6d473e202e1ca0d279f0`
at initialization. This does not inspect mapped library bytes. Main advanced
after this candidate, so it is not qualification of the updated base.

The first build used system Python 3.9 and failed before running tests because
the build script requires newer type syntax; its [log](build-python39-failure.log)
is retained. The successful runs used Python 3.12 and a freshly built test
binary. Base image IDs were
`sha256:cdff3048cba997d65c23596692fed5133696895473cdd2f53bb6b284f4787e95`
and `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
