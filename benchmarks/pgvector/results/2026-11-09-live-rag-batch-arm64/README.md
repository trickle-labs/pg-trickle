# Batched workload smoke test

The initial end-to-end test failed because its exact comparison included the
stream table's internal columns; see [initial-failure.log](initial-failure.log).
The comparison and documented example were corrected to select the public
corpus columns. The [retry](retry-smoke.log) passed three update cycles on a
four-row corpus using PostgreSQL 18.6, pgvector 0.8.7 and the ARM64 development
image `sha256:bc4a6094c8c84a38acb7c56ef4663be620067cedccd4f6fc5bbf67efbcb6ff8c`.
The test checks rotation, successful generation, exact corpora, retrieval,
refresh status and rejection of a second run in the same database. This is a
functional test, not evidence of performance at scale.

## 10,000-row rehearsal

Runner commit `b1d21131` completed three batches of ten updates at 384
dimensions on an Apple M1 Pro, Linux ARM64 Docker (10 CPUs, 8,319,504,384
bytes available), PostgreSQL 18.6 and pgvector 0.8.7. The image ID was
`sha256:e84deda6329086aeb490e1f69e6a2986f524cd4ee7a502bb6478c0c763ebdec4`.
It was built from the earlier v0.109.0 candidate archive recorded at commit
`fe2904bfae6e5031cf5df37cd53a2613b6ea92d3`, archive SHA-256
`370e325e65bd8f991f522c2ac90dac7d2bb506a2727956fb1240524d58ab366b`.
The installed `pg_trickle.so` SHA-256 was
`cab1a964f9cd313148882c5e214febbec6c27711d9ce033a813c1401ef104cf9`,
matching the earlier archive attestation. This run did not independently
attest every installed file. The source fixture and benchmark runner are
later than the packaged extension.

All three corpora matched the defining SQL after each batch. The initial
differential load fell back to FULL; the three measured updates reported
completed DIFFERENTIAL refreshes without full fallback. Median batch
maintenance times, including `psql` startup, were 0.048 s for application
updates, 0.559 s for differential refresh and 3.521 s for full recomputation;
median commit-to-eligible generation time was 0.671 s. There were only three
samples per path, and the serial order rotated. This observation is not a
general ranking or SLA.

The normal planner did not choose an ANN index in any of the recorded vector
queries. The seed produces repeated vectors and the exact top-10 boundary had
three tied rows, so the reported recall of 1.0 does not establish ANN quality.
See the [compressed JSON](10k-384.json.gz) for samples, timings, WAL movement,
sizes and plans, and the [log](10k-384.log) for fallback and refresh messages.
This single update-only scenario has no concurrent writers, delay distribution,
or CPU measurements and uses administrative current views. It is not final
candidate qualification.

After that rehearsal, the seed was changed to use a deterministic per-dimension
hash instead of the repeated-vector formula, and refresh reason and merge
strategy were added to subsequent reports. The four-row smoke test passed
with pgvector [0.8.1](hash-smoke-081.log) and [0.8.7](hash-smoke-087.log).
The 10,000-row results above use the earlier seed and report format.

The later mixed-churn mode passed its two-cycle, eight-row guarded-availability
test on pgvector [0.8.1](mixed-smoke-081.log) and [0.8.7](mixed-smoke-087.log).
Each four-slot batch covers an update, visibility change, deletion and
insertion. The test checks that the affected rows are withheld before the
new embeddings and refresh, and that each maintained corpus matches its
defining SQL afterward. This is functional evidence, not a scale result.
