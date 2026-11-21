# Current-main package centroid checks

On 2026-11-15, the seeded 1,536-dimensional churn test passed 2,000 refresh
calls on pgvector [0.8.1](drift-081.log) and [0.8.7](drift-087.log), using
test source `786ba8b95be807a91b65f4999c03764241acd446`. The package was
the v0.109.0 Linux ARM64 artifact `14161540423` from
[qualification run 43710039862](https://github.com/trickle-labs/pg-trickle/actions/runs/43710039862),
integrated commit `7e7b51c2a7f31cdf24d06408cd815e4b6b147a77`, archive
SHA-256 `90d121bfa63fb30197ce535a78be93367b0f8e72dad2ae52d06b0b9a9e94591b`.
It includes main through `8ae8e117` but predates the later CTE allocation
optimization and test changes. PostgreSQL 18.6 ran in Linux ARM64 Docker on
an Apple M1 Pro.

The test uses seed `0x7a139465`, varied signed and near-zero components, and
insert, update, delete, NULL and group-move changes. After every call it
compares groups, counts, NULLs, dimensions and components to native pgvector
AVG using `1e-5 + 1e-6 * abs(native)` tolerance. Each run recorded 1,962
latest-history algebraic observations, zero unbound refreshes, maximum
absolute error `1.2960299500264227e-11`, and maximum error/tolerance fraction
`1.29602948093257e-6`. The two versions ran concurrently on a small corpus;
these samples are not performance or capacity measurements.

The crash-recovery test with same-snapshot diagnostics from source
`a585375b06c9cbb093150766f81bc50d7a7f2139` also passed once on
[0.8.1](crash-081.log) and [0.8.7](crash-087.log). The added failure message
records actual and native values, binary equality, frontiers, materialized
transaction, strategy and refresh reason. Passing runs validate the test
query but do not explain the earlier hosted combined-assertion failure.

Installation records for the [0.8.1 churn](attest-081.json.gz),
[0.8.7 churn](attest-087.json.gz), [0.8.1 crash](attest-crash-081.json.gz),
and [0.8.7 crash](attest-crash-087.json.gz) tests each match all 157 candidate
files and payload digest
`ce7bf0734e7caf17b941a6c49507393670bf63960ef602c02cf217e0965d8913` at
container initialization. The checks do not inspect mapped library bytes.
The later integrated candidate still requires its own release qualification.
