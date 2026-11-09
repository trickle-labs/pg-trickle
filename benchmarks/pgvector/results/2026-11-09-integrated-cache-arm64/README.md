# Integrated development image: subscription and recovery smoke tests

On 2026-11-09, the integrated source commit
`3ee97525497f5a930c890d8f663304a781e29434` was built into a PostgreSQL 18.6
ARM64 development image on an Apple M1 Pro with Linux Docker. The pg_trickle
library SHA-256 was
`21b1f705eb5981e72dd109a9fa50a0b3f709c2c2049adb0c2aadbe12731763e0`.
This image was built from source, not from a release archive.

Six selected E2E tests passed for each of pgvector
[0.8.1](tests-081.log) and [0.8.7](tests-087.log): concurrent ANN reindex
with reads and refresh, centroid crash recovery, concurrent subscription
commit and rollback, duplicate subscriptions with a user trigger, membership
crossings, and typed-predicate authorization. The image IDs were
`sha256:746ef1d664d44e669b902048448bde1f7bd69a2c013e1fa1baffa672f30a345d`
and `sha256:5edb4e6092c64a9a0a1dbe867733725ca38fd11a7de3478d9ea08b97806ba2e3`,
respectively.

These are targeted development-image checks, not the candidate-bound release
qualification. The earlier hosted Linux crash-recovery failure remains
unexplained despite these passing repetitions.
