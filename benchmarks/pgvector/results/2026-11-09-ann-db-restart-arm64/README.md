# Database restart during packaged ANN maintenance

On 2026-11-09, the existing concurrent-reindex restart test passed against
the v0.109.0 ARM64 archive from
[qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The test and release-contract source were committed at
`c8da6f62433986a0bdc31190a5a63370943049bf`.

The [pgvector 0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log) runs
used PostgreSQL 18.6 on Linux ARM64 Docker on an Apple M1 Pro. Their images
were `sha256:cdff3048cba997d65c23596692fed5133696895473cdd2f53bb6b284f4787e95`
and `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The [0.8.1](attestation-081.json.gz) and [0.8.7](attestation-087.json.gz)
observations each matched all 157 candidate PostgreSQL payload files. The
one-request worker SQL was executed from the extracted candidate archive.

The test restarts the PostgreSQL container while concurrent reindex waits for
an old snapshot. It verifies an interrupted request and preserved cooldown,
commits and refreshes a new source row, retries the same worker after advancing
the cooldown, and verifies a valid rebuilt index and exact corpus. It also
checks that the interrupted invalid index remains visible for explicit operator
cleanup. The connection-reset notice during restart is expected.

This is a controlled restart of an earlier package, not an abrupt host crash
or proof that the supervisor itself reconnects automatically. The test is now
required in both v0.109.0 pgvector package suites; the final integrated
candidate still requires its own qualification.
