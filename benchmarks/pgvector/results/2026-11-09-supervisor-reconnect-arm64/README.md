# Supervisor reconnection after database restart

The test passed with pgvector 0.8.1 and 0.8.7 on Linux ARM64 using the
v0.109.0 archive at commit `1e291536bf0380b37a509b730155ca3d3a0ccea4`,
SHA-256 `2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The host was an Apple M1 Pro; PostgreSQL was 18.6. The supervisor and worker
were taken from that archive. Both installation observations match all 157
candidate payload files.

One supervisor process completed a request, remained alive through a forced
container stop and restart, and completed a different request with a valid
index afterward. Docker changed the host port on restart, so the test updated
the port in a libpq service file before the second request. Connection-refused
messages during the outage in the logs are expected. This checks reconnection
and renewed work, not restart while the supervisor has an active request;
the separate packaged worker test covers interruption and retry after restart.
The archive predates the final integrated candidate.
