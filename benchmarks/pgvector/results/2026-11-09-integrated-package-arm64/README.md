# Integrated ARM64 package: targeted install and upgrade checks

The ARM64 archive from [qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708)
has embedded candidate commit `1e291536bf0380b37a509b730155ca3d3a0ccea4`
and archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
Its five operator scripts and documents were byte-compared with that commit.
The normalized PostgreSQL payload has 157 files and digest
`0cf46931a74ff096067c1000fddcf70d78362b2f4a25b9f9f3a7aaad39f84001`.
The host was an Apple M1 Pro using Linux ARM64 Docker and PostgreSQL 18.6.

The [upgrade rehearsal](upgrade.log) used the published v0.108.3 ARM64 archive
with SHA-256 `498bbe3bc7415a22389d767c0b0fae4d96aa0618c7e64704c500af934835cf99`,
matching its published checksum file. It upgraded directly to this v0.109.0
archive with pgvector 0.8.7. The existing vector stream and L2 index, inactive
legacy subscription, default-off centroid state, pending committed changes,
durable consumer data and WAL-slot retirement checks passed. The
[installation observation](upgrade-attestation.json.gz) matched every
candidate payload file and recorded both archive digest and commit.

Fresh PostgreSQL test images were built from the same archive with pinned
pgvector 0.8.1 and 0.8.7. Ten targeted tests passed in each image:

- Installed pgvector version; supervised ANN worker recovery; and HNSW and
  IVFFlat concurrent-rebuild reads and refreshes. The worker and supervisor
  used scripts extracted from the candidate archive.
- Randomized centroid churn; crash/restart rollback and catch-up; corruption;
  missing state; repeated rewrite/rebase suppression; numerically unsafe
  unbound state; and concurrent writer serialization.

The test source was integrated commit `599c5cc3aaff9970ff1ecdacc917d90703520415`,
later than the packaged extension. Full logs and ten per-container
installation observations are retained for [0.8.1](fresh-081.log)
([attestations](fresh-081-attestations.json.gz)) and [0.8.7](fresh-087.log)
([attestations](fresh-087-attestations.json.gz)). All twenty installed file
lists and digests matched the candidate payload and recorded its commit and
archive digest. Image IDs were
`sha256:cdff3048cba997d65c23596692fed5133696895473cdd2f53bb6b284f4787e95`
and `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`,
respectively.

These are targeted direct-suite runs, not the complete machine-enforced
release report. The hosted macOS build was canceled and the hosted package
qualification stage was skipped. This archive also predates the latest
integrated candidate; no cross-platform or final-release sign-off follows
from these results.
