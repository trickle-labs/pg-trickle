# Packaged pgvector suites, 2026-11-08

These runs used the clean checkout at `d0d63391` for the test harness and
PostgreSQL 18.6 arm64 containers on an Apple M1 Pro. The pre-existing images
reported pg_trickle 0.110.0. Their arm64 image IDs were:

- pgvector 0.8.1: `sha256:fa8d212543d317b7410017c31783e4f6bf02beb67ab7521a1d6ebea1b3b3c9ed`
- pgvector 0.8.7: `sha256:8a481cd18588ef2b1f645e583f13538f987254782b571c1ba0ed20257d50967c`

The image labels contain no source revision, so these results identify the
installed binaries but do **not** prove that they were built from this commit.
Rebuild and rerun with verifiable provenance before using them as release
qualification. The test suites each include three shared oracle unit tests;
the totals below are not exclusively database tests.

| pgvector | Main suite | Hybrid suite | Revision-safe suite |
|---|---:|---:|---:|
| 0.8.1 | 128 passed | 6 passed | 7 passed |
| 0.8.7 | 128 passed | 6 passed | 7 passed |

No tests were ignored or filtered. The main suite used two test threads for
0.8.7 and one for 0.8.1. Both recipe suites used two threads. Commands, with
`VERSION` replaced by each version and `THREADS` by the values above:

```sh
PGS_E2E_PLATFORM=linux/arm64 PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:$VERSION \
  cargo test --test e2e_pgvector_tests -- --test-threads=$THREADS
PGS_E2E_PLATFORM=linux/arm64 PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:$VERSION \
  cargo test --test e2e_pgvector_hybrid_tests --test e2e_pgvector_revision_tests -- --test-threads=2
```

The host also supplied its Docker socket through `DOCKER_HOST` and used a
Python 3 installation compatible with the schema generator. Logs:
[main 0.8.1](pgvector-0.8.1.log), [main 0.8.7](pgvector-0.8.7.log),
[recipes 0.8.1](recipes-0.8.1.log), and [recipes 0.8.7](recipes-0.8.7.log).
These tests do not resolve the intermittent benchmark failures in the
[rewrite report](../2026-11-08-rewrite-centroid-arm64/README.md), qualify a
centroid cost threshold, or replace the repository's full test and upgrade tiers.
