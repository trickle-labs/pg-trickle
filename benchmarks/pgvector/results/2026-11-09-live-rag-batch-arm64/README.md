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
