# Centroid cost sample against a newer package

On 2026-11-15, the opt-in comparison passed on PostgreSQL 18.6 and pgvector
0.8.7 in Linux ARM64 Docker on an Apple M1 Pro. The v0.109.0 archive was from
[qualification run 43710039862](https://github.com/trickle-labs/pg-trickle/actions/runs/43710039862),
commit `7e7b51c2a7f31cdf24d06408cd815e4b6b147a77`, artifact `14161540423`,
SHA-256 `90d121bfa63fb30197ce535a78be93367b0f8e72dad2ae52d06b0b9a9e94591b`.
The [installation observation](installation.json.gz) matched all 157 candidate
and installed files and payload digests at startup. The image ID was
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The checkout was `80ec2133c4f9fea9fa701714bb750b7621a30c55`; the reused test
binary came from `1e143564`, whose Rust and benchmark-test sources are
identical. This is not qualification of the final release artifact.

The workload began with 5,000 deterministic 768-dimensional vectors. After
the earlier refresh workload deleted 500 rows, the small and large groups had
45 and 2,250 rows. Ten rounds changed one vector each; group and strategy
order rotated. Each refresh checked its selected strategy and compared the
affected groups against native pgvector AVG within the documented absolute
and relative bounds. The largest recorded output component error was zero at
the comparison precision.

| Group | Group rescan median | Algebraic median | FULL median |
| --- | ---: | ---: | ---: |
| 45 rows | 14.01 ms | 22.68 ms | 44.89 ms |
| 2,250 rows | 31.98 ms | 21.61 ms | 43.57 ms |

Each median has ten observations. [The complete result](result.json) includes
the individual samples, initial instrumented plans, tuple counts, WAL and
logical state-byte estimates; the [test log](run.log.gz) retains its output.
Refresh-call timings exclude source mutation and validation. There was no
explicit warmup or controlled concurrent load; the first instrumented
comparison used a fixed order. These small, synthetic samples show a crossover
in this workload but do not calibrate the automatic selector, measure rebase
frequency or establish a general numerical or performance envelope.
