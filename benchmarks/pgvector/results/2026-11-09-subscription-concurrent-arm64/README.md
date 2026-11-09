# Concurrent distance notifications against an earlier candidate

The bounded test used 100 rows and ten subscriptions; the opt-in scale test
used 10,000 rows and 100 subscriptions. Each used three-dimensional vectors,
one pivot, two L2 thresholds and one real LISTEN connection subscribed to a
distinct channel per subscription. An open refresh transaction and a second
source-writer transaction overlapped. The test verified that uncommitted
refreshes produced no notification and remained invisible to another reader;
after commits, every channel received the expected enter counts. A no-op
membership change and a rolled-back exit produced no observed notification;
rollback preserved source and stream values. Deleting the last two matching
rows produced the expected exit count for every channel, left no matching
rows and the final stream matched its defining SQL. Payloads were under the
PostgreSQL notification size limit, and channels were checked for duplicates.

Both tests passed with PostgreSQL 18.6, pg_trickle 0.109.0 and pgvector
[0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log) on Linux ARM64 Docker
on an Apple M1 Pro. Each log includes post-commit listener-drain times, which
begin after the commit or refresh call returns and can include already-buffered
notifications. The two version runs overlapped, and each case has one sample;
these values are not end-to-end latency, capacity or SLA estimates.

The candidate archive was the earlier integrated qualification artifact at
commit `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
Its 157-file normalized PostgreSQL payload digest was
`0cf46931a74ff096067c1000fddcf70d78362b2f4a25b9f9f3a7aaad39f84001`.
Both test-container observations per version matched that payload and recorded
the candidate commit and archive digest: [0.8.1](pgvector-081-attestations.json.gz),
[0.8.7](pgvector-087-attestations.json.gz). The candidate-derived image IDs were
`sha256:cdff3048cba997d65c23596692fed5133696895473cdd2f53bb6b284f4787e95`
and `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`,
respectively. The tests were added after that archive was built.

This is a controlled semantic test, not sustained concurrent load or a full
subscription scaling matrix. It does not cover tenant authorization, several
pivots, 384/768-dimensional vectors, restart/retry, or the final candidate
artifact. NOTIFY remains best-effort and is not durable delivery.
