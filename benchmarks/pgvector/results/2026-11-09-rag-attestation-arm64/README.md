# Live RAG archive attestation smoke test

On 2026-11-09, a 100-document, 384-dimensional mixed batch ran against
PostgreSQL 18.6 and pgvector 0.8.7 in Linux ARM64 Docker on an Apple M1 Pro.
It used the v0.109.0 archive from
[qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The test source builds on `c9fa22b6`.

The [result](smoke.json.gz) records that all 157 installed extension files
matched the archive at both ends of the run, the container and benchmark
connections identified the same PostgreSQL instance, and all three corpora
matched defining SQL initially and after the batch. The normalized mapping of
installed paths to SHA-256 values had digest
`cda2c7ccbee945367150af9374a229c620befedae3e3cc5b72b4dc0e4ad7f1e8`.
The archive manifest itself supplied the candidate commit.

The [negative controls](negative-controls.json) rejected an incorrect archive
digest, an otherwise identical package on a different PostgreSQL instance,
and an altered installed control file. A separate bad-digest runner invocation
failed before creating either benchmark schema in its disposable database.

This smoke test checks the attestation path on an earlier candidate; it is not
the final release qualification or a performance comparison. The checks
compare files on disk and sample database identity before and after the run;
they do not inspect library bytes already mapped by a backend or prove that a
load-balanced connection uses the same instance for every intermediate query.
