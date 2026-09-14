# PLAN_DOCKER_IMAGE.md — Official Docker Image

> **Status:** INFRA-1 implemented (build CI + smoke test, no push yet)
> **Target version:** v1.0.0 (push to Docker Hub)
> **Author:** pg_trickle project
>
> **Implemented in v0.7.0:** `Dockerfile.ghcr` (end-user image pinned to a
> PostgreSQL 18.3 digest), `.github/workflows/ghcr.yml`, and
> `.github/workflows/docker-hub.yml` (weekly build + smoke test, no push).
> At v1.0.0 set `push: true` and add
> `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` secrets.

---

## 1. Overview

Publish an official Docker image to Docker Hub at
`pgtrickle/pg_trickle:<tag>` containing PostgreSQL 18 with the pg_trickle
extension pre-installed and pre-configured.

The image is aimed at:
- Quick local evaluation (`docker run`)
- CI/CD matrices (replace `postgres:18` with `pgtrickle/pg_trickle:pg18`)
- Kubernetes deployments (base image for CNPG `additionalPlugins`)

---

## 2. Base Image Choice

| Option | Size (approx) | Notes |
|--------|--------------|-------|
| `postgres:18-bookworm` | ~430 MB | Official, well-maintained, Debian |
| `postgres:18-alpine` | ~240 MB | Smaller but glibc compat issues with Rust `.so` |
| Custom Ubuntu 24.04 | ~600 MB | More control, easier Rust toolchain |

**Decision:** `postgres:18-bookworm` — official, stable, avoids glibc issues.

---

## 3. Dockerfile

```dockerfile
# syntax=docker/dockerfile:1
ARG PG_VERSION=18

# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM rust:1.84-bookworm AS builder

ARG PG_VERSION
RUN apt-get update && apt-get install -y \
      postgresql-server-dev-${PG_VERSION} \
      libclang-dev clang pkg-config && \
    rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-pgrx --version 0.17 --locked && \
    cargo pgrx init --pg${PG_VERSION} $(which pg_config)

WORKDIR /build
COPY . .
RUN cargo pgrx package --pg-config $(which pg_config) \
      --out-dir /tmp/pkg

# ── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM postgres:${PG_VERSION}-bookworm

ARG PG_VERSION

COPY --from=builder /tmp/pkg/usr/lib/postgresql/${PG_VERSION}/lib/pg_trickle.so \
     /usr/lib/postgresql/${PG_VERSION}/lib/

COPY --from=builder /tmp/pkg/usr/share/postgresql/${PG_VERSION}/extension/ \
     /usr/share/postgresql/${PG_VERSION}/extension/

# Auto-load the extension for convenience in development
RUN echo "shared_preload_libraries = 'pg_trickle'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "pg_trickle.enabled = on"                 >> /usr/share/postgresql/postgresql.conf.sample

COPY docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/

HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
  CMD pg_isready -U "${POSTGRES_USER:-postgres}" || exit 1
```

### `docker-entrypoint-initdb.d/01-pg_trickle.sql`

```sql
CREATE EXTENSION IF NOT EXISTS pg_trickle;
SELECT pgtrickle.version();
```

---

## 4. Multi-arch Builds

Use Docker Buildx to produce `linux/amd64` and `linux/arm64` manifests:

```bash
docker buildx create --use --name multi-arch
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag pgtrickle/pg_trickle:1.0.0-pg18 \
  --tag pgtrickle/pg_trickle:pg18 \
  --tag pgtrickle/pg_trickle:latest \
  --push .
```

---

## 5. Image Tag Strategy

| Tag | Meaning |
|-----|---------|
| `latest` | Latest stable + latest supported PG |
| `pg18` | Latest stable on PostgreSQL 18 |
| `1.0.0-pg18` | Exact pg_trickle version + PG version |
| `0.2.0-pg18` | Pinned pre-release |

Major releases also get a floating `1-pg18` tag pointing to the latest 1.x.y.

---

## 6. GitHub Actions Workflow

```yaml
# .github/workflows/docker.yml
name: Docker

on:
  push:
    tags: ['v*']

jobs:
  push:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4

      - name: Set up QEMU
        uses: docker/setup-qemu-action@v3

      - name: Set up Buildx
        uses: docker/setup-buildx-action@v3

      - name: Login to Docker Hub
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKERHUB_USERNAME }}
          password: ${{ secrets.DOCKERHUB_TOKEN }}

      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          platforms: linux/amd64,linux/arm64
          push: true
          tags: |
            pgtrickle/pg_trickle:latest
            pgtrickle/pg_trickle:pg18
            pgtrickle/pg_trickle:${{ github.ref_name }}-pg18
```

---

## 7. Relationship to Existing Dockerfiles

| File | Purpose | Reuse |
|------|---------|-------|
| [tests/Dockerfile.e2e](../../tests/Dockerfile.e2e) | E2E test image | Build stage can share the Rust build layer |
| [tests/Dockerfile.e2e-coverage](../../tests/Dockerfile.e2e-coverage) | Coverage instrumented | Coverage-only |
| [cnpg/Dockerfile](../../cnpg/Dockerfile) | CNPG overlay | CNPG-specific; separate image |

The `builder` stage in the official image should be kept in sync with
`tests/Dockerfile.e2e` to avoid build drift.

---

## 8. Smoke Test

```bash
docker run --rm -e POSTGRES_PASSWORD=test \
  pgtrickle/pg_trickle:latest \
  postgres -c "shared_preload_libraries=pg_trickle" &

sleep 3
docker exec <cid> psql -U postgres -c "
  CREATE EXTENSION pg_trickle;
  CREATE TABLE src (id int primary key, v int);
  SELECT pgtrickle.create_stream_table('agg', 'SELECT sum(v) FROM src');
  SELECT pgtrickle.version();
"
```

---

## References

- [cnpg/Dockerfile](../../cnpg/Dockerfile)
- [tests/Dockerfile.e2e](../../tests/Dockerfile.e2e)
- [PLAN_PACKAGING.md](PLAN_PACKAGING.md)
- [PLAN_GITHUB_ACTIONS_COST.md](PLAN_GITHUB_ACTIONS_COST.md)
