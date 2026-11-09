# Abrupt supervisor replacement on a candidate package

On 2026-11-09, the extended supervisor test at source commit
`10b09a925f03b9e74ec45155ccf7a675754671b5` passed with pgvector
[0.8.1](pgvector-081.log.gz) and [0.8.7](pgvector-087.log.gz) on PostgreSQL 18.6,
Linux ARM64 Docker and an Apple M1 Pro. Both images were built from the
v0.109.0 ARM64 archive from
[qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The image IDs were
`sha256:cdff3048cba997d65c23596692fed5133696895473cdd2f53bb6b284f4787e95`
and `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The [0.8.1](attestation-081.json.gz) and [0.8.7](attestation-087.json.gz)
installation observations each matched all 157 candidate PostgreSQL payload
files. The supervisor and worker scripts ran from the extracted archive and
matched the corresponding test-source files byte for byte.

The test holds a concurrent rebuild behind a table lock, kills the supervisor
without signaling its active worker, and starts a replacement supervisor. It
observes exactly one blocked worker, releases the lock, and verifies that the
original claim epoch succeeds and the index is valid. The same test also
exercises graceful supervisor shutdown, revoked and restored privilege, worker
death, cooldown and retry. The cancellation error in each log is expected from
the graceful-shutdown case.

This covers a live worker surviving its supervisor, not a simultaneous
process-group kill or database restart. It uses a controlled lock barrier and
a ten-second statement timeout, not sustained production load. The archive is
an earlier candidate and does not qualify the final integrated release.
