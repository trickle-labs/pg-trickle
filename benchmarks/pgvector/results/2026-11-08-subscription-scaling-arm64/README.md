# Distance subscription scaling smoke run

On 2026-11-08, PostgreSQL 18.6, pgvector 0.8.7 and pg_trickle 0.110.0 ran
in a Linux arm64 container on an Apple M1 Pro host. The image ID was
`sha256:cb86ed4e235ac6e25c89e9549af363018d3f01e68e9472a75d00a92b363d1f8b`.
It was built from the source later committed as `87838942` and has no source
revision label. The final harness was an uncommitted change on `1c35d429`;
its SHA-256 was
`87bdb2f4a4fccdbf93b5eb87b98343940ecc72cefc7fd58b68807d997c58e39a`.

Each case used a three-dimensional vector and five one-row changes. The final
run analyzed each stream, checked the differential strategy and one-row
snapshot, and used a listener to verify each subscription's enter or exit
event after commit. The timing covers the refresh call only: mutation, commit,
listener delivery and validation are excluded. There was no warmup, controlled
load or randomized case order. Tuple counters count visits, not distinct rows.

| Initial rows | Subscriptions | Median refresh ms | Stream sequential tuples returned per refresh |
|---:|---:|---:|---:|
| 1,000 | 0 | 12.39 | 1,000 |
| 1,000 | 1 | 13.86 | 0 |
| 1,000 | 10 | 18.06 | 0 |
| 10,000 | 0 | 11.64 | 0 |
| 10,000 | 1 | 14.14 | 0 |
| 10,000 | 10 | 17.69 | 0 |

The analyzed subscription cases recorded `explicit_dml`; controls recorded
`delete_insert`. A preceding exploratory run without ANALYZE recorded one
stream-table-sized set of sequential tuple returns per subscribed refresh and
two such sets in the zero-subscription controls. That earlier harness did not
record merge strategy or attach a listener, so the scans cannot be attributed
to notification evaluation. This shows that a one-row snapshot alone does not
prove the whole refresh avoids a scan. The [analyzed](analyzed.json) and
[unanalysed](control.json) reports contain all samples; neither establishes a
general latency, scaling or recall guarantee.
