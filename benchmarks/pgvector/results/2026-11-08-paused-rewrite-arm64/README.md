# Scheduler-isolated centroid rewrite smoke run

On 2026-11-08 the packaged workload ran on an Apple M1 Pro with 32 GiB host
memory, macOS 27.2 and Docker Server 29.9.2. PostgreSQL 18.6, pgvector 0.8.7
and pg_trickle 0.110.0 ran in a Linux arm64 container. The image ID was
`sha256:cb86ed4e235ac6e25c89e9549af363018d3f01e68e9472a75d00a92b363d1f8b`,
built from source later committed as `87838942`; it has no source revision
label. The harness was commit `7fdf79c0`, with SHA-256
`9b7e7b417c7b5e3fddbf1d4523b2a123a0fdd364bee7ac06c38a0abcea0faaca`.

The workload started with 5,000 deterministic 384-dimensional vectors. After
deleting 10%, it ran 20 rounds changing 200 rows across 100 small and two large
groups. For each round, it refreshed a group-rescan, an algebraic, and a FULL
stream for each group shape, checked the actual strategy, and compared all
affected groups with native AVG using `1e-5 + 1e-6 * |AVG|` tolerance. The
scheduler was paused for each benchmark stream. All 120 refresh observations
and assertions passed. The [raw report](384.json) includes the measurements,
plans and version information.

This was one run without controlled host load or explicit warmup. The timings
do not establish a general performance threshold, and this run does not explain
the earlier failures documented in the [rewrite report](../2026-11-08-rewrite-centroid-arm64/README.md).
