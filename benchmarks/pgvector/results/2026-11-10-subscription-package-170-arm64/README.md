# Subscription tests against an integrated package

On 2026-11-10, the distance-subscription E2E selection ran on PostgreSQL
18.6, Linux ARM64 Docker on an Apple M1 Pro, with pgvector 0.8.1 and 0.8.7.
The installed v0.109.0 package was the Linux ARM64 artifact from
[qualification run 42730543726](https://github.com/trickle-labs/pg-trickle/actions/runs/42730543726),
source commit `170c3b9c7d65383265baf73723fbca3de4e3cbe5`, artifact ID
`13724619170`, archive SHA-256
`7c80a8295a196142594fee1ddf79da72fa52f69b49e6060be03412cd3e388444`.
The package was copied over the extension files in the test image before
container startup. The compressed records below match all 157 candidate
files and payload digests at initialization; they do not inspect mapped
library bytes. This is an earlier integrated candidate, not the latest one.

The test source was `d8c0498334a10e09e34bfd51e313ca97effe96b5`. The
rebuilt selection passed 21 tests with one larger test ignored on
[0.8.1](suite-fresh-081.log) and [0.8.7](suite-fresh-087.log), including
duplicate predicates, trigger-side changes, overlapping source transactions,
commit and rollback, typed predicates, authorization, and last-row exits.
The opt-in [0.8.1](scale-081.log) and [0.8.7](scale-087.log) cases each
passed separately with 10,000 rows, 100 subscriptions and three-dimensional
vectors. They use a real listener, check notification payloads and channels,
and test commit visibility, concurrent source writes and rollback. This is
not a sustained-load or representative-dimension benchmark.

Installation records: [suite 0.8.1](attest-fresh-081.json.gz),
[suite 0.8.7](attest-fresh-087.json.gz), [scale 0.8.1](attest-scale-081.json.gz),
[scale 0.8.7](attest-scale-087.json.gz). The suite records contain two
container observations each. Base image IDs were
`sha256:cdff3048cba997d65c23596692fed5133696895473cdd2f53bb6b284f4787e95`
and `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`
respectively. PostgreSQL and pgvector versions were checked in the running
containers; the scale test also asserts the expected pgvector version.

The first [0.8.1](suite-081.log) and [0.8.7](suite-087.log) invocations
passed 18 tests, but reused a binary from another worktree because the Cargo
target directory was shared. Their [0.8.1](attest-081.json.gz) and
[0.8.7](attest-087.json.gz) records contain an abbreviated source SHA. These
initial invocations did not execute the new tests and are retained only as
diagnostic history; the test source was touched and rebuilt for the results
above. Neither run establishes a final-release qualification or scale limit.
