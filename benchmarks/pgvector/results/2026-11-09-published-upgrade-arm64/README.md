# Published-package upgrade rehearsal, ARM64

On an Apple M1 Pro using Linux ARM64 Docker and PostgreSQL 18.6 with pgvector
0.8.7, the published v0.108.3 archive was upgraded directly to a v0.109.0
qualification-run artifact. The published archive SHA-256 was
`498bbe3bc7415a22389d767c0b0fae4d96aa0618c7e64704c500af934835cf99`,
matching the published `SHA256SUMS.txt`. The candidate archive SHA-256 was
`370e325e65bd8f991f522c2ac90dac7d2bb506a2727956fb1240524d58ab366b`;
its embedded metadata identifies commit
`fe2904bfae6e5031cf5df37cd53a2613b6ea92d3` and [qualification run
42584171042](https://github.com/trickle-labs/pg-trickle/actions/runs/42584171042).

The first attempt reached the upgraded vector checks but failed because the
verification query did not qualify an ambiguous `status` column. Its
[failure log](initial-failure.log) is retained. The rerun used the corrected
verification script at commit `37b74276fd37d8fc8796c9634f3a8dcfc97d7845`
and passed; see [rehearsal.log](rehearsal.log). It checked the existing L2
index and stream, inactive legacy subscriptions, default-off centroid state,
pending committed changes, durable output consumer data, and WAL-slot
retirement rollback and committed cleanup. The [installation attestation](install-attestation.json)
records matching candidate and installed payload digests for 157 files. Its
candidate-commit and archive-identity fields are null: the archive SHA and
embedded metadata above were verified separately, not bound into that record.

The same candidate archive was also installed into fresh PostgreSQL 18.6
containers for the 11 declared pgvector cases with each of pgvector 0.8.1 and
0.8.7. Both runs passed all 11 cases: [0.8.1 log](fresh-081.log) and
[0.8.7 log](fresh-087.log). Each run's compressed installation attestations
([0.8.1](fresh-081-attestations.json.gz), [0.8.7](fresh-087-attestations.json.gz))
contain 11 observations whose candidate and installed payload digests match.
These local direct-suite runs are not a full machine-enforced release report.

This is evidence for that earlier ARM64 artifact and these specific checks.
It does not qualify the later combined candidate or replace the full release
recovery and platform matrix. The associated hosted qualification run was
canceled because its macOS build did not complete.
