# Centroid cost samples across dimensions

On 2026-11-15, the opt-in comparison passed at 384 and 1,536 dimensions on
PostgreSQL 18.6 with pgvector 0.8.7 in Linux ARM64 Docker on an Apple M1 Pro.
Both tests installed the v0.109.0 archive from
[qualification run 43710039862](https://github.com/trickle-labs/pg-trickle/actions/runs/43710039862),
commit `7e7b51c2a7f31cdf24d06408cd815e4b6b147a77`, artifact `14161540423`,
SHA-256 `90d121bfa63fb30197ce535a78be93367b0f8e72dad2ae52d06b0b9a9e94591b`.
The [384](attest-384.json.gz) and [1,536](attest-1536.json.gz) installation
observations each matched 157 candidate and installed files and payload
digests at startup. The image ID was
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The checkout was `9f45e0a7f5deba89684a18c3c98143d921e6babb`; the reused test
binary came from `1e143564`, whose Rust and benchmark-test sources are
identical. The archive is earlier than the final integrated candidate.

Each workload began with 5,000 deterministic vectors. Following 500 deletes,
the small and large groups contained 45 and 2,250 rows. Ten rounds changed
one vector each, rotating group and strategy order. Each refresh checked the
selected strategy and all affected groups against native pgvector AVG using
the documented absolute and relative bounds; maximum recorded component error
was zero at comparison precision. The table also includes the separately
recorded [768-dimensional sample](../2026-11-15-centroid-cost-768-package-arm64/README.md).

| Dimensions | Group rows | Group rescan median | Algebraic median | FULL median |
| ---: | ---: | ---: | ---: | ---: |
| 384 | 45 | 14.36 ms | 21.13 ms | 22.61 ms |
| 384 | 2,250 | 20.51 ms | 21.05 ms | 21.93 ms |
| 768 | 45 | 14.01 ms | 22.68 ms | 44.89 ms |
| 768 | 2,250 | 31.98 ms | 21.61 ms | 43.57 ms |
| 1,536 | 45 | 14.08 ms | 23.50 ms | 74.63 ms |
| 1,536 | 2,250 | 45.49 ms | 22.53 ms | 72.86 ms |

Each median contains ten observations. [384](384.json) and [1,536](1536.json)
retain individual samples, initial instrumented plans, tuple counts, WAL and
logical state-byte estimates; [384](run-384.log.gz) and
[1,536](run-1536.log.gz) logs retain the test output. Refresh-call timings
exclude source mutation and validation. The 768 run preceded the 384 and
1,536 runs, which ran sequentially. There was no explicit warmup or controlled
concurrent load; PostgreSQL reported skipped autovacuum attempts during the
1,536 run. These synthetic samples do not calibrate the automatic selector,
measure rebase frequency or establish a general operating envelope.
