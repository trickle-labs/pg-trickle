# 100,000-row subscription matrix against the v0.109.0 package

The existing ignored subscription benchmark ran on 2026-11-15 from source
`17ca7bcd` against the Linux ARM64 v0.109.0 archive built from
`7e7b51c2a7f31cdf24d06408cd815e4b6b147a77` (SHA-256
`90d121bfa63fb30197ce535a78be93367b0f8e72dad2ae52d06b0b9a9e94591b`). It used
PostgreSQL 18.6, pgvector 0.8.7, and Docker image
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`
on an Apple M1 Pro with 32 GiB RAM. The [run log](run.log) contains all three
rounds; [measurements.json](measurements.json) is the raw JSON output.

The analyzed 100,000-row source and stream used 384-dimensional vectors. One
source row alternated between two vectors across the threshold; the 1, 10, 100
and 1,000 subscriptions used distinct nearby pivots. Every expected
notification arrived, every subscribed refresh used a one-row bounded
snapshot, and the test required a differential refresh. The zero-subscription
case used `delete_insert`; subscribed cases used `explicit_dml`.

| Subscriptions | Merge strategy | Refresh samples (ms) | Median (ms) | Listener wait samples (ms) |
| ---: | --- | --- | ---: | --- |
| 0 | delete_insert | 21.54, 15.91, 13.07 | 15.91 | — |
| 1 | explicit_dml | 15.83, 14.94, 31.05 | 15.83 | 0.046, 0.043, 0.058 |
| 10 | explicit_dml | 22.39, 20.00, 18.54 | 20.00 | 0.181, 0.144, 0.151 |
| 100 | explicit_dml | 56.58, 55.88, 56.27 | 56.27 | 1.043, 1.043, 1.072 |
| 1,000 | explicit_dml | 442.46, 452.01, 433.88 | 442.46 | 8.673, 8.741, 8.720 |

The zero-subscription control has a different merge strategy, and prior case
tables remain in this one database as the sweep progresses. Do not use these
serial samples to estimate a per-subscription cost or compare them as an
isolated scaling curve. There was no concurrent traffic, and the single
deterministic changed row does not represent general vector distributions.

The [installation attestation](attestation.json.gz) verifies a fresh PostgreSQL
instance and identical digests for all 157 candidate and installed package
files. The package is an earlier integrated candidate, not the final release
candidate. For the plan-level explanation of full stream visits in the earlier
exploratory case, see the [instrumented subscription controls](../2026-11-09-subscription-controls-arm64/README.md)
and the [10,000-row analyzed/unanalyzed plan excerpts](../2026-11-15-subscription-768-package-arm64/README.md).
