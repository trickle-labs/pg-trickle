# Crash-recovery investigation on an x86_64 archive

The [PostgreSQL 18 / pgvector 0.8.7 CI job](https://github.com/trickle-labs/pg-trickle/actions/runs/42613795217/job/129148547560)
failed its combined post-crash centroid assertion. That assertion did not report
which of the vector value, native AVG, frontier, transaction state or selected
strategy differed; the test now prints the individual values on failure.

On 2026-11-09, five [recorded repetitions](crash-recovery.log) passed against
the linux-amd64 archive from
[qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708).
The archive reports candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`
and SHA-256
`5a99cf20e5134ae5749196b95b74c9321782b947e2e8458e0ae004e5c2eed1fd`.
The extracted and installed `pg_trickle.so` SHA-256 matched:
`8bf14c99d781c69f0661e1906f09a639d35bb0ef15c71eea481a01680d03308b`.
The pgvector 0.8.7 test image ID was
`sha256:72ba26e809e5d3b2ba7aed1ac32814240abcfc710a1cf1809299a700df1ac598`.
Tests used the later integrated test source at `2322e5520488f5ec27651891ac3998c425bb182e`.

The host was an Apple M1 Pro running the x86_64 PostgreSQL 18.6 container under
Docker emulation. Fifteen additional unrecorded repetitions also passed. This
is an earlier archive and a different environment from the native GitHub
runner; passing repetitions do not establish the root cause or qualify the
latest release candidate. The hosted failure remains unresolved.
