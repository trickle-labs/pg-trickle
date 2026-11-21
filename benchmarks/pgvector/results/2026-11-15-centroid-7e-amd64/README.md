# Centroid crash recovery against a later x86 package

The earlier [hosted crash assertion](https://github.com/trickle-labs/pg-trickle/actions/runs/42613795217/job/129148547560)
did not identify which part of its combined assertion failed. The test now
reports the individual values and same-snapshot state on failure.

The same crash-recovery case passed on native x86 in the pgvector 0.8.7 suite
of [qualification run 43710039862](https://github.com/trickle-labs/pg-trickle/actions/runs/43710039862),
which used integrated commit `7e7b51c2a7f31cdf24d06408cd815e4b6b147a77`.
On 2026-11-15, four further repetitions passed locally on an Apple M1 Pro
using an emulated Linux amd64 PostgreSQL 18.6 container and pgvector 0.8.7.
The [logs](crash-repeat-1.log) for runs [2](crash-repeat-2.log),
[3](crash-repeat-3.log) and [4](crash-repeat-4.log) are retained.

The local tests installed the Linux amd64 archive from that qualification,
artifact `14161715058`, SHA-256
`a8f43298d047fb4bac52f2058ec2fc9a5352d620f9abef36643c49214f53d6f8`.
All four [installation observations](installation.json.gz) match 157 candidate
and installed files and their payload digests. The test image was
`sha256:72ba26e809e5d3b2ba7aed1ac32814240abcfc710a1cf1809299a700df1ac598`;
the test source was `e8e3873fde3f4ad68f8ee9f6762fa5241f0fed24`.

This is an earlier candidate and the local host uses emulation. Passing runs
do not identify the root cause of the original hosted failure or qualify the
latest release artifact; the original failure remains unconfirmed.
