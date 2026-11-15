# One-million-row subscription scale observations

The existing ignored subscription benchmark ran on 2026-11-15 from source
`17ca7bcdaa835345c3ed2bee839327e57328fca4` against the Linux ARM64 v0.109.0
archive built from `7e7b51c2a7f31cdf24d06408cd815e4b6b147a77` (SHA-256
`90d121bfa63fb30197ce535a78be93367b0f8e72dad2ae52d06b0b9a9e94591b`). It used
PostgreSQL 18.6, pgvector 0.8.7, and Docker image
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`
on an Apple M1 Pro with 32 GiB RAM. Each count ran serially in a fresh
container with one million 384-dimensional rows and one changed source row.
Each case has one sample. Its [JSON output](subscriptions-1000.json) and
[full test log](subscriptions-1000.log) are retained alongside a separate
installation attestation for every case.

| Subscriptions | Merge strategy | Refresh (ms) | Snapshot | Stream sequential tuples returned | Listener wait (ms) |
| ---: | --- | ---: | --- | ---: | ---: |
| 0 | delete_insert | 36.61 | none | 0 | — |
| 1 | explicit_dml | 35.32 | 1-row bounded | 0 | 0.051 |
| 10 | explicit_dml | 36.96 | 1-row bounded | 0 | 0.139 |
| 100 | explicit_dml | 62.99 | 1-row bounded | 0 | 0.921 |
| 1,000 | explicit_dml | 508.93 | 1-row bounded | 0 | 8.694 |

Every expected notification arrived and each refresh was differential. The
zero-subscription control uses a different merge strategy. These single
synthetic observations are not percentiles, a controlled per-subscription
comparison, a capacity limit or an SLA. They used a deterministic corpus with
identical initial vectors, no concurrent traffic, and no nested-plan
instrumentation. The 1,000-subscription case measures one changed row and does
not qualify other update shapes or query plans.

Each [attestation](attest-1000.json.gz) records a fresh PostgreSQL instance and
matching digests for all 157 candidate and installed files. The package is an
earlier integrated candidate, not the final release candidate. Previous
instrumented explanation of full-stream visits is recorded in the
[subscription controls](../2026-11-09-subscription-controls-arm64/README.md)
and the [10,000-row package matrix](../2026-11-15-subscription-768-package-arm64/README.md).

One additional analyzed 1,000-subscription run enabled nested `auto_explain`
with actual rows and buffers. It passed with a one-row bounded snapshot and
recorded zero sequential tuples returned from the stream. The [measured plan
summary](subscription-plans.json) retains one representative bounded-snapshot
plan and one membership plan, with totals across all 1,000 membership
statements: 5,000 shared-buffer hits and zero reads. The snapshot plan recorded
one shared hit and three shared reads. The measured interval contained 5,167
plan records and 58,874 shared-buffer counter fields; the [complete PostgreSQL
log](postgres-plans.log.gz) is retained (63.8 MB raw, 7.3 MB compressed).
Instrumented refresh took 835.9 ms, compared with 508.9 ms in the separate
un-instrumented single sample; instrumentation changes the timing, so this is
plan evidence rather than a performance comparison. Its separate [test
log](subscriptions-1000-instrumented.log) and [package
attestation](attest-1000-instrumented.json.gz) are retained.
