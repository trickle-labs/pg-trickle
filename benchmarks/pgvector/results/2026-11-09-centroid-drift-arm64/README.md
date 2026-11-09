# Seeded centroid drift smoke run

This run exercises 2,000 seeded mutation and refresh calls using the test at
commit `5132f5076ed10f07eaf1e50a0e087ff6f3576d56`. Its initial 30 rows use
three-dimensional vectors across five groups; the deterministic workload
includes upserts, deletes, group moves, NULL vectors, and cancellation between
positive and negative components. After every call the test compares every
group and component with a fresh native pgvector `avg`, using the bound
`1e-5 + 1e-6 * abs(native_component)` and checking group counts and NULLs.

The complete checkpoint report is in [drift.json](drift.json). The maximum
observed component error was zero at the output's float32 precision. The
latest completed history entry reported the algebraic strategy at 1,962 of
the 2,000 observations; this is not a count of distinct refreshes because a
mutation can be a no-op. No component exceeded the stated bound.

The development image used PostgreSQL 18.6, pgvector 0.8.7 and pg_trickle
0.109.0 on an Apple M1 Pro with 32 GiB RAM, using a Linux arm64 Docker
container. The image ID was
`sha256:36731f289865bbd35c7f591ebad9641c6546dd91a2be2693a88ab5e6ff12bab9`;
the pg_trickle shared library SHA-256 was
`0b236a96c80eaae16e855f5133d993481c77cd0acecef0d1de98d0fba4696f86`.
The extension sources match the test commit, but the image is not a release
artifact.

This single seed and small three-dimensional dataset do not establish safety
for extreme inputs, higher dimensions, joins, restart/recovery, concurrency,
or long-term drift on other workloads. They do not calibrate the cost selector
or establish performance; those qualification requirements remain open.
