# Supervisor process-group death and recovery

The release-required supervisor test passed on pgvector 0.8.1 and 0.8.7 with
PostgreSQL 18.6 on Linux ARM64. It used the v0.109.0 archive at commit
`1e291536bf0380b37a509b730155ca3d3a0ccea4`, SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
Both installation observations match all 157 candidate files, and the
supervisor and worker scripts were taken from the archive. The host was an
Apple M1 Pro.

The test places the supervisor in a separate process group and blocks its
concurrent rebuild behind a table lock. It kills the group, waits for the
request to become INTERRUPTED with a cooldown, releases the lock, advances the
cooldown for the test, and starts a replacement supervisor. The retry succeeds
and leaves a valid index. The worker uses a ten-second statement timeout, so
server-side cleanup is bounded even when the client is killed. This is a
single controlled failure per version, not a sustained fault or load test;
the archive predates the final integrated candidate.
