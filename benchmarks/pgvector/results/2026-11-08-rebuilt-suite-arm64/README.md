# Rebuilt packaged pgvector suites, 2026-11-08

The clean checkout was at `a878de6b23062aa07ac77ed76a934859b66714a4` when
the packaged extension was built and the test harness was run. The build used
`tests/Dockerfile.e2e` with the local ARM64 `pg_trickle_builder:pg18` image
(`sha256:67b573db7e461ee48157e3690fa4054a276623568a5af364c2ddd0994e120e3d`).
The base image ID was
`sha256:2bc0ae70c7c8ac732e2d8dbf9159bcdd625268a816e2b4189b1224d6a555ee56`.
The build explicitly labelled the base image with the checkout revision; the
version-specific images inherited that label.

The pgvector layers used the upstream image digests from
`tests/build_e2e_pgvector_image.sh`. A temporary copy of
`tests/Dockerfile.e2e-pgvector` replaced its base with the newly built image,
leaving existing local tags unchanged. The resulting ARM64 image IDs were:

- pgvector 0.8.1: `sha256:d5667419c38733013adea14ad2c17565ac73711fb3b4ecb82c7cd1465cb35cda`
- pgvector 0.8.7: `sha256:5b287f617939c9fe1d692e0a6dc9d0d26f5c337b554da85d49188155da2d87a3`

The build ran on an Apple M1 Pro using PostgreSQL 18.6 containers. Image IDs,
the recorded source checkout, and the [base](base-build.log),
[0.8.1](vector-0.8.1-build.log), and [0.8.7](vector-0.8.7-build.log) build
logs identify this build; the local builder tag and package repositories are
not pinned by the recipe, so this is not a claim of bit-for-bit reproducibility.
ANSI colors were removed from the archived build logs.

| pgvector | Main suite | Hybrid suite | Revision-safe suite |
|---|---:|---:|---:|
| 0.8.1 | 128 passed | 6 passed | 7 passed |
| 0.8.7 | 128 passed | 6 passed | 7 passed |

No tests were ignored or filtered. Each suite includes three shared oracle
unit tests; the totals are not exclusively database tests. For each version,
the command below ran with two test threads and the corresponding image tag:

```sh
PGS_E2E_PLATFORM=linux/arm64 PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:$VERSION-a878de6b \
  cargo test --test e2e_pgvector_tests --test e2e_pgvector_hybrid_tests \
    --test e2e_pgvector_revision_tests -- --test-threads=2
```

The host supplied its Docker socket through `DOCKER_HOST` and used a Python 3
installation compatible with the schema generator. See the complete
[0.8.1 test log](tests-0.8.1.log) and [0.8.7 test log](tests-0.8.7.log).
These runs strengthen the provenance of the earlier
[suite evidence](../2026-11-08-packaged-suite-arm64/README.md). They do not
resolve the intermittent benchmark failures in the
[rewrite report](../2026-11-08-rewrite-centroid-arm64/README.md), qualify a
centroid cost threshold, or replace the full repository and upgrade tiers.
