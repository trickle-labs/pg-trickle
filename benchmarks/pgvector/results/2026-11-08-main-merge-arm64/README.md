# Packaged pgvector conformance after the main merge

On 2026-11-08, PostgreSQL 18 and pg_trickle 0.110.0 ran in Linux arm64
containers on an Apple M1 Pro host. The package was built from the source
subsequently committed as `0dcb4a94`; the images do not carry a source revision
label. Tests ran from that commit. The pinned pgvector 0.8.7 image ID was
`sha256:29fff085c0b9f56af7a9c7cd16af4c0842fa3427614ef934132aa14d7b5e763d`
and the 0.8.1 image ID was
`sha256:74edcb6390294116a178fe4af68660efc8b76994d6d99460b5090e03a968091d`.

Each version ran `cargo test --test e2e_pgvector_tests -- --test-threads=2`
(133 passed), `cargo test --test e2e_pgvector_hybrid_tests -- --test-threads=2`
(6 passed), and `cargo test --test e2e_pgvector_revision_tests -- --test-threads=2`
(7 passed). The two version suites ran concurrently. Host activity and
container resources were not controlled.

The 0.109.0-to-0.110.0 upgrade regression
`test_upgrade_embedding_helpers_from_0109` ran with `--ignored --exact` and
passed once against upgrade image
`sha256:2393a5a99891ed683b24f2f4900d7a8302ca4fef1122872d45eb6af10d068cab`.
The remaining upgrade tests were filtered out. The logs in this directory
record the individual test names and results; these local runs do not replace
the repository's complete CI gates.

The targeted 0.109.0-to-0.110.0 legacy distance-subscription and ANN epoch
rollback regressions also passed once each. The non-ignored catalog suite
initially failed because its expected column list omitted the three ANN epoch
and churn columns. After updating that test expectation, the suite passed
18 tests with 21 ignored. The before/after logs preserve the failure and rerun;
the final test source includes that expectation change beyond `0dcb4a94`.

The repository's `just test-integration` suite also passed 161 tests using
stock PostgreSQL 18.6 Testcontainers and four test threads. This suite tests
catalog and DVM fixtures without loading the packaged extension. An initial
build failed after another checkout shared its Cargo target directory; cleaning
the `pg_trickle` package artifacts and rebuilding resolved the stale-library
field mismatch. The successful run is recorded in [integration.log](integration.log).
