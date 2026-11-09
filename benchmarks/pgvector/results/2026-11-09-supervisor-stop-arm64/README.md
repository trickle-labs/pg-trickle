# Supervisor stop and recovery smoke test

The first local test used supervisor commit `797bd37bda8c86fee8bb44a0fd76f45425ed15fa`.
After a request was claimed while a table lock was held, stopping the
supervisor with SIGTERM left the worker backend active beyond the 15-second
test limit. The [failure log](initial-failure.log) is retained.

The fix at commit `255439e35a870bec1c4a5d209dc370fa6fa8cc2b` sends SIGINT
to active `psql`, which requests server-side query cancellation. The expanded
test witnesses the reindex blocked by the held lock before stopping the
supervisor, requires the backend to exit while the lock is still held, then
restarts with MAINTAIN revoked. The request stays unclaimed until access is
restored. The test advances the persisted cooldown explicitly to avoid a
real-time wait, then verifies retry success, a valid target index, unchanged
unrelated index filenode, and the committed corpus against its defining query.

The test passed with PostgreSQL 18.6 and pgvector [0.8.1](pgvector-081.log)
and [0.8.7](pgvector-087.log) on Linux ARM64 Docker on an Apple M1 Pro. The
development image IDs were
`sha256:620d26b64447b4dce1960f8fca396b6f4b2b985763cb4f75cb67d42d45261212`
and `sha256:bc4a6094c8c84a38acb7c56ef4663be620067cedccd4f6fc5bbf67efbcb6ff8c`,
respectively. Both used the pg_trickle library SHA-256
`de9d0d7e81f058ad67014a70aded4119733cf23990fb3d2def33b3b2e24ced6f`.

This checks graceful supervisor shutdown during a blocked operation, not
abrupt supervisor death, database restart, or natural cooldown duration. The
images are development builds rather than the final release artifact.
