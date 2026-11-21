# Supervisor retry after database restart during a rebuild

The release-required supervisor test passed with pgvector 0.8.1 and 0.8.7 on
Linux ARM64 using PostgreSQL 18.6 and the v0.109.0 candidate archive at commit
`1e291536bf0380b37a509b730155ca3d3a0ccea4`, SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The host was an Apple M1 Pro. Both installation observations matched all 157
candidate payload files, and the supervisor and worker came from the archive.

The supervisor completed one request, then a table lock held its next
concurrent rebuild while the PostgreSQL container was forcibly stopped and
restarted. The request became INTERRUPTED and retained a cooldown. The test
updated a libpq service file for Docker's remapped port and advanced the
cooldown; the same supervisor retried the request and produced a valid index,
then completed another request. Connection errors in the logs during the
outage are expected. These are single controlled tests, not sustained fault
or load coverage, and the archive predates the final integrated candidate.
