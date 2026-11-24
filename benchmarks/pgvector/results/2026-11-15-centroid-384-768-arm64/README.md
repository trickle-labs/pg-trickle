# Diverse centroid churn at 384 and 768 dimensions

On 2026-11-15, test source `786ba8b95be807a91b65f4999c03764241acd446`
ran on PostgreSQL 18.6 with pgvector 0.8.1 and 0.8.7 in Linux ARM64 Docker
on an Apple M1 Pro. The installed v0.109.0 archive was artifact `13734025185`
from [qualification run 42734523338](https://github.com/trickle-labs/pg-trickle/actions/runs/42734523338),
integrated commit `9a0e39a121001b1729ec9ab082499bd8e432cfdd`, with SHA-256
`27c3421626d83fa83fc0b427a39e5dc6674254a1588ce6674bf3c9ba078074db`.
Main advanced after this package was built; it is not the latest candidate.

The deterministic seed `0x7a139465` drives 2,000 individual insert, update,
delete, NULL and group-move refresh calls per case. Components vary across
vectors and include large signed and near-zero values. After each call, the
test compares groups, counts, NULLs, dimensions, and every component against
native pgvector AVG using `1e-5 + 1e-6 * abs(native)` tolerance. It checks
the state remains bound and records algebraic history observations.

The two versions for each dimension ran concurrently in separate containers.
The corpus is small and elapsed times are not performance measurements.
These runs cover one deterministic seed and do not establish the full
numerical, recovery, or workload envelope.

| Dimensions | pgvector | Test log | Installation record |
| ---: | --- | --- | --- |
| 384 | 0.8.1 | [log](drift-384-081.log) | [record](attest-384-081.json.gz) |
| 384 | 0.8.7 | [log](drift-384-087.log) | [record](attest-384-087.json.gz) |
| 768 | 0.8.1 | [log](drift-768-081.log) | [record](attest-768-081.json.gz) |
| 768 | 0.8.7 | [log](drift-768-087.log) | [record](attest-768-087.json.gz) |

All four cases passed. Each reported 1,962 latest-history algebraic
observations and zero unbound refreshes. The maximum absolute component
error was `1.2960299500264227e-11`, with maximum error/tolerance fraction
`1.29602948093257e-6`. Each installation record matches all 157 files and
the payload digest `1fed301b8ff98ccf9922e91bf48a4579552a84da4ebd6d473e202e1ca0d279f0`
at initialization; it does not inspect mapped library bytes.

Formatting and lint passed on the test source. An initial lint attempt
failed because dynamically generated SQL lacked SQLx's `AssertSqlSafe`;
the dimension is now restricted to the four explicit test values and the
corrected source passed lint and all four tests above.
