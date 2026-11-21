# ANN supervisor with separated client roles

On 2026-11-15, the short and sustained supervisor tests passed on PostgreSQL
18.6 with pgvector 0.8.1 and 0.8.7 in ARM64 Docker on an Apple M1 Pro. The
test source was `9732bfe1a33ed8073277d5e93643d4ed81ce5692`. The tests used
the v0.109.0 Linux ARM64 package from
[qualification run 43710039862](https://github.com/trickle-labs/pg-trickle/actions/runs/43710039862),
commit `7e7b51c2a7f31cdf24d06408cd815e4b6b147a77`, artifact `14161540423`,
archive SHA-256 `90d121bfa63fb30197ce535a78be93367b0f8e72dad2ae52d06b0b9a9e94591b`.
The packaged supervisor and worker scripts were used. All four compressed
installation observations below match 157 candidate and installed files and
payload digests at container initialization.

| pgvector | Short test | Sustained test | Installation records |
| --- | --- | --- | --- |
| 0.8.1 | [log](short-081.log.gz) | [log](sustained-081.log.gz) | [short](attest-short-081.json.gz), [sustained](attest-sustained-081.json.gz) |
| 0.8.7 | [log](short-087.log.gz) | [log](sustained-087.log.gz) | [short](attest-short-087.json.gz), [sustained](attest-sustained-087.json.gz) |

For HNSW and IVFFlat separately, the supervisor used a non-superuser role with
`SELECT`, `MAINTAIN` and the required API grants, but no direct queue access.
An administrator connection assumed distinct restricted roles for source
inserts and stream reads. The test checked that the reader could not insert
into the source and the writer could not select from the stream. In the
sustained case, 120 paced source transactions, refreshes and indexed reads
completed for each index while its concurrent rebuild waited on an old
snapshot; the final stream matched the source.

The test uses a small corpus, an artificial snapshot barrier and an
administrator connection that assumes the client roles; it is not a capacity
test or evidence of separate authenticated reader and writer connections.
The image IDs were
`sha256:cdff3048cba997d65c23596692fed5133696895473cdd2f53bb6b284f4787e95`
and `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The package predates the final integrated candidate and must be requalified.
