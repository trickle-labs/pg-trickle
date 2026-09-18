# CloudNativePG Image Volume Extension — Implementation Plan

Date: 2026-02-26
Status: IMPLEMENTED
Standalone implementation plan for the CloudNativePG image-volume extension.
PR: [#15](https://github.com/trickle-labs/pg-trickle/pull/15)
Branch: `cloudnative-pg-image-volume`

---

## Implementation Summary

All 9 tasks were implemented in PR #15. Key decisions made during
implementation:

| Decision | Chosen Approach |
|---|---|
| Release smoke test | **Both Option A + B** — layout verification via `docker create`/`docker cp`, plus a composite-image SQL smoke test |
| CI CNPG smoke test | **Strategy A (transitional)** — composite image (`scratch` ext + `postgres:18.1`) until `kind` supports K8s 1.33 |
| CNPG operator version in CI | Remains at **1.25.0** (latest stable at time of implementation) |
| Base `INSTALL.md` version | Uses `0.1.0` (current release) rather than `0.2.0` |

Files changed:

| Action | File |
|--------|------|
| Created | `cnpg/Dockerfile.ext` |
| Created | `cnpg/Dockerfile.ext-build` |
| Created | `cnpg/database-example.yaml` |
| Deleted | `cnpg/Dockerfile` |
| Deleted | `cnpg/Dockerfile.release` |
| Modified | `cnpg/cluster-example.yaml` |
| Modified | `.github/workflows/release.yml` |
| Modified | `.github/workflows/ci.yml` |
| Modified | `justfile` |
| Modified | `INSTALL.md` |
| Modified | `docs/RELEASE.md` |
| Modified | `docs/introduction.md` |

---

## Overview

Replace the current full-PostgreSQL Docker image (`ghcr.io/<owner>/pg_trickle`)
with a minimal, `scratch`-based **extension-only** OCI image
(`ghcr.io/<owner>/pg_trickle-ext`) that follows the
[CloudNativePG Image Volume Extensions](https://cloudnative-pg.io/docs/1.28/imagevolume_extensions/)
specification.

The extension image contains **only** the `.so`, `.control`, and `.sql` files —
no PostgreSQL server, no operating system, no shell. Users pair it with the
official CNPG minimal PostgreSQL 18 operand images and declare the extension in
their `Cluster` resource via `.spec.postgresql.extensions`.

### Motivation

- **Decoupled distribution.** The extension lifecycle is independent of the
  PostgreSQL server image. Users adopt official, hardened, minimal PG images
  from the CNPG project and overlay only the extension files they need.
- **Smaller surface area.** A `scratch`-based image is < 10 MB (just the `.so`
  and SQL files) vs. ~400 MB for the current `postgres:18.1`-based image.
- **Supply-chain security.** Immutable, signed extension images with no OS
  packages to patch. Base PG image updates are handled by the CNPG project.
- **Kubernetes-native.** Leverages the Kubernetes 1.33 `ImageVolume` feature
  gate for read-only, immutable volume mounts — no init containers, no
  `emptyDir` hacks.

### Requirements

| Component | Minimum Version |
|---|---|
| PostgreSQL | 18 (`extension_control_path` GUC) |
| Kubernetes | 1.33 (`ImageVolume` feature gate) |
| Container runtime | containerd ≥ 2.1.0 or CRI-O ≥ 1.31 |
| CloudNativePG operator | 1.28+ |

---

## Table of Contents

- [Architecture](#architecture)
- [Image Specification](#image-specification)
- [Implementation Tasks](#implementation-tasks)
  - [Task 1 — Extension-Only Dockerfile (scratch)](#task-1--extension-only-dockerfile-scratch)
  - [Task 2 — From-Source Build Dockerfile](#task-2--from-source-build-dockerfile)
  - [Task 3 — Remove Old Full-Image Dockerfiles](#task-3--remove-old-full-image-dockerfiles)
  - [Task 4 — Update Release Workflow](#task-4--update-release-workflow)
  - [Task 5 — Update CI CNPG Smoke Test](#task-5--update-ci-cnpg-smoke-test)
  - [Task 6 — Rewrite Cluster Example Manifests](#task-6--rewrite-cluster-example-manifests)
  - [Task 7 — Update Justfile](#task-7--update-justfile)
  - [Task 8 — Update Documentation](#task-8--update-documentation)
  - [Task 9 — Update Dependabot & Path Triggers](#task-9--update-dependabot--path-triggers)
- [Verification](#verification)
- [Rollback Plan](#rollback-plan)
- [ADR — Full Image vs Extension Image](#adr--full-image-vs-extension-image)

---

## Architecture

### How It Works (CNPG Image Volume Flow)

```
┌─────────────────────────────────────────────────────────────────────┐
│  Cluster CR (.spec.postgresql.extensions)                           │
│                                                                     │
│  extensions:                                                        │
│    - name: pg-trickle                                                │
│      image:                                                         │
│        reference: ghcr.io/<owner>/pg_trickle-ext:0.2.0               │
│                                                                     │
│  shared_preload_libraries: [pg_trickle]                              │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  CNPG Operator                                                      │
│                                                                     │
│  1. Triggers rolling update                                         │
│  2. Mounts extension image as ImageVolume at:                       │
│       /extensions/pg-trickle/                                        │
│  3. Appends to postgresql.conf:                                     │
│       extension_control_path = '..:/extensions/pg-trickle/share'     │
│       dynamic_library_path   = '..:/extensions/pg-trickle/lib'       │
│  4. Starts PostgreSQL                                               │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PostgreSQL 18 Pod                                                  │
│                                                                     │
│  /extensions/pg-trickle/                                             │
│  ├── lib/                                                           │
│  │   └── pg_trickle.so          ← shared library                     │
│  └── share/                                                         │
│      └── extension/                                                 │
│          ├── pg_trickle.control  ← extension control file            │
│          └── pg_trickle--0.2.0.sql  ← SQL migration                  │
│                                                                     │
│  CREATE EXTENSION pg_trickle;   ← works via extension_control_path   │
└─────────────────────────────────────────────────────────────────────┘
```

### Image Contents (from scratch)

```
/
├── lib/
│   └── pg_trickle.so
└── share/
    └── extension/
        ├── pg_trickle.control
        └── pg_trickle--<version>.sql
```

This layout follows the CNPG default convention. The operator automatically
appends `/extensions/pg-trickle/share` to `extension_control_path` and
`/extensions/pg-trickle/lib` to `dynamic_library_path`.

---

## Image Specification

| Property | Value |
|---|---|
| Base image | `scratch` |
| Image name | `ghcr.io/<owner>/pg_trickle-ext` |
| Tag scheme | `<version>` (e.g. `0.2.0`), `<major.minor>` (e.g. `0.2`), `latest` |
| Architectures | `linux/amd64`, `linux/arm64` |
| OCI labels | `title`, `description`, `licenses`, `source`, `version` |
| Expected size | < 10 MB |

### Compatibility

The `.so` is compiled against a specific PostgreSQL major version, OS
distribution, and CPU architecture. Each published image tag implicitly targets:

- **PostgreSQL 18** (compiled with `pg_config` from `postgres:18.x`)
- **Debian** (glibc-linked — compatible with the official CNPG Debian-based operand images)
- **amd64** or **arm64** (multi-arch manifest)

Users must match the operand image family. Using an Alpine-based operand image
with a Debian-compiled extension will fail at runtime.

---

## Implementation Tasks

### Task 1 — Extension-Only Dockerfile (scratch)

**File:** `cnpg/Dockerfile.ext`
**Status:** ✅ Completed

This Dockerfile is used by the **release workflow** to package pre-built
artifacts (already compiled in the `build-release` job) into the extension
image. No Rust compilation happens here.

```dockerfile
# =============================================================================
# Extension-only image for CloudNativePG Image Volume Extensions.
#
# Contains ONLY the pg_trickle shared library, control file, and SQL
# migrations. Designed to be mounted as an ImageVolume in a CNPG Cluster.
#
# Usage (from dist/ context with pre-built artifacts in artifact/):
#   docker build -t pg_trickle-ext:latest -f cnpg/Dockerfile.ext dist/
# =============================================================================
FROM scratch

ARG REPO_URL=https://github.com/trickle-labs/pg-trickle
ARG VERSION=dev

# Extension shared library
COPY artifact/lib/*.so /lib/

# Extension control + SQL migration files
COPY artifact/extension/ /share/extension/

# OCI labels
LABEL org.opencontainers.image.title="pg_trickle-ext" \
      org.opencontainers.image.description="pg_trickle extension for CloudNativePG Image Volume Extensions" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="${REPO_URL}"
```

### Task 2 — From-Source Build Dockerfile

**File:** `cnpg/Dockerfile.ext-build`
**Status:** ✅ Completed

Multi-stage Dockerfile for local development and CI. Stage 1 compiles the
extension from source; Stage 2 copies only the extension files into `scratch`.

> **Note:** The dependency-caching layer creates stub `src/ivm/` directories.
> The actual source tree uses `src/dvm/`. This doesn't break the build (the
> `COPY src/ src/` layer overwrites the stubs), but reduces cache hit rate.
> A follow-up PR should rename the stubs to `src/dvm/`.

```dockerfile
# =============================================================================
# Multi-stage build: compiles pg_trickle from source, then produces a
# scratch-based extension image for CNPG Image Volume Extensions.
#
# Usage (from project root):
#   docker build -t pg_trickle-ext:latest -f cnpg/Dockerfile.ext-build .
# =============================================================================

# ── Stage 1: Build the extension ────────────────────────────────────────────
FROM postgres:18.1 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl build-essential libreadline-dev zlib1g-dev pkg-config \
        libssl-dev libclang-dev clang postgresql-server-dev-18 \
        ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

RUN cargo install --locked cargo-pgrx --version 0.17.0
RUN cargo pgrx init --pg18 /usr/bin/pg_config

WORKDIR /build
COPY Cargo.toml ./

# Dependency caching layer
RUN mkdir -p src/bin src/ivm/operators benches && \
    echo '#![allow(warnings)] fn main() {}' > src/bin/pgrx_embed.rs && \
    echo '#![allow(warnings)]' > src/lib.rs && \
    echo '' > src/ivm/mod.rs && \
    echo '' > src/ivm/operators/mod.rs && \
    echo 'fn main() {}' > benches/refresh_bench.rs && \
    echo 'fn main() {}' > benches/diff_operators.rs && \
    cargo generate-lockfile && \
    cargo fetch

COPY src/ src/
COPY pg_trickle.control ./

RUN cargo pgrx package --pg-config /usr/bin/pg_config

# Verify artifacts exist
RUN find target/release/pg_trickle-pg18 -type f

# ── Stage 2: Scratch extension image ───────────────────────────────────────
FROM scratch

# Copy shared library
COPY --from=builder \
    /build/target/release/pg_trickle-pg18/usr/lib/postgresql/18/lib/ \
    /lib/

# Copy extension control + SQL files
COPY --from=builder \
    /build/target/release/pg_trickle-pg18/usr/share/postgresql/18/extension/ \
    /share/extension/

LABEL org.opencontainers.image.title="pg_trickle-ext" \
      org.opencontainers.image.description="pg_trickle extension for CloudNativePG Image Volume Extensions" \
      org.opencontainers.image.licenses="Apache-2.0"
```

### Task 3 — Remove Old Full-Image Dockerfiles

**Files deleted:**
- `cnpg/Dockerfile` — full PostgreSQL image with UID remapping
- `cnpg/Dockerfile.release` — full PostgreSQL image from pre-built artifacts

**Status:** ✅ Completed

These are fully replaced by `Dockerfile.ext` and `Dockerfile.ext-build`. The
UID 26 remapping, the `postgres:18.1` base, and the extension file placement
into `/usr/lib/postgresql/18/lib/` are all unnecessary when using Image Volumes.

### Task 4 — Update Release Workflow

**File:** `.github/workflows/release.yml`
**Status:** ✅ Completed — Both Option A and Option B were implemented.

Changes:

1. **Rename `IMAGE_NAME`** from `${{ github.repository_owner }}/pg_trickle` to
   `${{ github.repository_owner }}/pg_trickle-ext`.

2. **`test-release` job** — The smoke test currently builds a full Docker image
   and runs `CREATE EXTENSION` inside it. Since the extension image is
   `scratch`-based (no shell, no PostgreSQL), the Docker-based SQL smoke test
   must use a different strategy:

   **Option A (implemented):** Keep the existing artifact-level verification
   (extract tar, check file tree), then validate the Docker image layout:
   ```bash
   docker build -t pg_trickle-ext:test -f cnpg/Dockerfile.ext dist/
   ID=$(docker create pg_trickle-ext:test true)
   docker cp "$ID:/lib/" /tmp/ext-lib/
   docker cp "$ID:/share/" /tmp/ext-share/
   docker rm "$ID"
   # Verify expected files exist
   test -f /tmp/ext-lib/pg_trickle.so
   test -f /tmp/ext-share/extension/pg_trickle.control
   ls /tmp/ext-share/extension/pg_trickle--*.sql
   ```
   **Option B (also implemented):** A composite image is also built in the
   same job for a SQL-level `CREATE EXTENSION` smoke test:
   ```dockerfile
   FROM postgres:18.1
   COPY --from=pg_trickle-ext:test /lib/ /usr/lib/postgresql/18/lib/
   COPY --from=pg_trickle-ext:test /share/extension/ /usr/share/postgresql/18/extension/
   ```
   Both options were implemented: Option A validates the image layout, then
   Option B runs a full `CREATE EXTENSION` + `SELECT` smoke test using the
   composite image.

3. **`publish-docker-arch` job** — Change:
   - `file: cnpg/Dockerfile.release` → `file: cnpg/Dockerfile.ext`
   - Update OCI labels (`title`, `description`) to reference the extension image
   - Update tags to use the new `IMAGE_NAME`

4. **`publish-docker` job** — Update manifest tags to use new `IMAGE_NAME`.
   Same tag scheme: `<version>`, `<major.minor>`, `latest`.

### Task 5 — Update CI CNPG Smoke Test

**File:** `.github/workflows/ci.yml` (lines ~252–420)
**Status:** ✅ Completed — Strategy A (transitional composite image)

The old smoke test built the full Docker image, loaded it into kind, and
deployed a `Cluster` with `imageName`. The new approach:

1. Build the extension image from source: `docker build -f cnpg/Dockerfile.ext-build`
2. Load the extension image into kind
3. Deploy a `Cluster` using the official CNPG PG 18 operand image as the base
4. Reference the extension image via `.spec.postgresql.extensions`
5. Run the existing SQL smoke test

**Blocker:** Kubernetes 1.33 with the `ImageVolume` feature gate enabled is
required. As of this writing, `kind` may not yet support K8s 1.33. Two
mitigation strategies:

- **Strategy A (implemented — transitional):** Keep a CI-only composite
  Dockerfile that copies extension files from the `scratch` image into
  `postgres:18.1`, used only for the smoke test. This validates the extension
  works but does not exercise the Image Volume path end-to-end.
  ```dockerfile
  # tests/Dockerfile.cnpg-smoke (CI-only, not shipped)
  FROM pg_trickle-ext:ci AS ext
  FROM postgres:18.1
  COPY --from=ext /lib/ /usr/lib/postgresql/18/lib/
  COPY --from=ext /share/extension/ /usr/share/postgresql/18/extension/
  ```
- **Strategy B (deferred):** Skip the CNPG smoke test until kind supports K8s
  1.33. The release pipeline still validates the image layout (Task 4).

The CI workflow additionally verifies the extension image layout before
creating the composite image (same `docker create`/`docker cp` approach as
the release workflow). Timeout was increased to 20 minutes.

> **TODO:** Update the CNPG operator version from `1.25.0` to `1.28.x` and
> switch to native `.spec.postgresql.extensions` once `kind` supports K8s 1.33
> with the `ImageVolume` feature gate.

### Task 6 — Rewrite Cluster Example Manifests

**Files:** `cnpg/cluster-example.yaml`, `cnpg/database-example.yaml`
**Status:** ✅ Completed

Replaced the current monolithic example with an Image Volume-based deployment.
The new manifest uses the official CNPG operand image as the base and
references the extension image via `.spec.postgresql.extensions`.

```yaml
# =============================================================================
# Example CloudNativePG Cluster with pg_trickle via Image Volume Extensions.
#
# Prerequisites:
#   1. Kubernetes 1.33+ with ImageVolume feature gate enabled
#   2. CNPG operator 1.28+ installed
#   3. pg_trickle-ext image available in the cluster registry
#
# Deploy:
#   kubectl apply -f cnpg/cluster-example.yaml
#   kubectl apply -f cnpg/database-example.yaml
# =============================================================================
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pg-trickle-demo
  labels:
    app.kubernetes.io/name: pg-trickle
    app.kubernetes.io/component: database
spec:
  instances: 3

  # Use the official CNPG minimal PostgreSQL 18 operand image.
  # The pg_trickle extension is loaded via Image Volumes below.
  imageName: ghcr.io/cloudnative-pg/postgresql:18

  postgresql:
    # Required: load the extension shared library at server start.
    shared_preload_libraries:
      - pg_trickle

    # Extension image — mounted at /extensions/pg-trickle/
    extensions:
      - name: pg-trickle
        image:
          reference: ghcr.io/<owner>/pg_trickle-ext:<version>

    parameters:
      # Recommended: scheduler bgworker + refresh workers
      max_worker_processes: "8"

  storage:
    size: 10Gi
    storageClass: standard
```

Add a separate `Database` resource for declarative extension management:

```yaml
# cnpg/database-example.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Database
metadata:
  name: pg-trickle-app
spec:
  name: app
  owner: app
  cluster:
    name: pg-trickle-demo
  extensions:
    - name: pg_trickle
```

**Changes from the old manifest:**
- `imageName` → official CNPG PG 18 image (no custom image)
- Added `.spec.postgresql.extensions` stanza
- Removed `bootstrap.initdb.postInitSQL` for `CREATE EXTENSION` — replaced by
  declarative `Database` resource

### Task 7 — Update Justfile

**File:** `justfile`
**Status:** ✅ Completed

```just
# ── Docker ────────────────────────────────────────────────────────────────

# Build the CNPG extension image (from source)
docker-build:
    docker build -t pg_trickle-ext:latest -f cnpg/Dockerfile.ext-build .
```

### Task 8 — Update Documentation

**Status:** ✅ Completed — updated INSTALL.md, docs/RELEASE.md, docs/introduction.md

#### INSTALL.md (lines 57–65)

Replace the "Using the Docker image" section:

```markdown
### 3. Using with CloudNativePG (Kubernetes)

pg_trickle is distributed as an OCI extension image for use with
[CloudNativePG Image Volume Extensions](https://cloudnative-pg.io/docs/1.28/imagevolume_extensions/).

**Requirements:** Kubernetes 1.33+, CNPG 1.28+, PostgreSQL 18.

```bash
# Pull the extension image
docker pull ghcr.io/trickle-labs/pg_trickle-ext:0.1.0
```

See [cnpg/cluster-example.yaml](cnpg/cluster-example.yaml) and
[cnpg/database-example.yaml](cnpg/database-example.yaml) for complete
Cluster and Database deployment examples.

For local Docker development without Kubernetes, install the extension files
manually into a standard PostgreSQL container:

```bash
# Extract extension files from the release archive
tar xzf pg_trickle-0.1.0-pg18-linux-amd64.tar.gz
cd pg_trickle-0.1.0-pg18-linux-amd64

# Run PostgreSQL with the extension mounted
docker run --rm \
  -v $PWD/lib/pg_trickle.so:/usr/lib/postgresql/18/lib/pg_trickle.so:ro \
  -v $PWD/extension/:/tmp/ext/:ro \
  -e POSTGRES_PASSWORD=postgres \
  postgres:18.1 \
  sh -c 'cp /tmp/ext/* /usr/share/postgresql/18/extension/ && \
         exec postgres -c shared_preload_libraries=pg_trickle'
```
```

#### docs/RELEASE.md (line 122)

Update the release artifacts table:

| Artifact | Description |
|----------|-------------|
| `ghcr.io/trickle-labs/pg_trickle-ext:<ver>` | CNPG extension image (amd64 + arm64) |

Update the Docker verification instructions to use the new image name and
layout validation (since the image has no shell, use `docker create` + `docker cp`).

#### docs/introduction.md (line 52)

Update feature table entry:

```
CloudNativePG-ready — Ships as an Image Volume extension image for Kubernetes
```

#### README.md

Update any references to the Docker image name.

### Task 9 — Update Dependabot & Path Triggers

**Files:**
- `.github/dependabot.yml` — ensure `cnpg/` directory is still watched
- `.github/workflows/ci.yml` — ensure `cnpg/**` path filter covers new filenames
- `.github/workflows/build.yml` — update any Docker-related path triggers

**Effort:** ~15 minutes

---

## Verification

### Local Build

```bash
# Build extension image from source
docker build -t pg_trickle-ext:test -f cnpg/Dockerfile.ext-build .

# Verify image size (should be < 10 MB)
docker images pg_trickle-ext:test

# Verify file layout
ID=$(docker create pg_trickle-ext:test true)
docker cp "$ID:/lib/" /tmp/ext-lib/
docker cp "$ID:/share/" /tmp/ext-share/
docker rm "$ID"

test -f /tmp/ext-lib/pg_trickle.so
test -f /tmp/ext-share/extension/pg_trickle.control
ls /tmp/ext-share/extension/pg_trickle--*.sql

echo "Extension image layout verified."
```

### Functional Test (requires K8s 1.33 cluster)

```bash
# Deploy CNPG cluster with extension
kubectl apply -f cnpg/cluster-example.yaml
kubectl apply -f cnpg/database-example.yaml

# Wait for cluster
kubectl wait cluster/pg-trickle-demo --for=condition=Ready --timeout=300s

# Verify extension is available
kubectl exec pg-trickle-demo-1 -- \
  psql -U postgres -d app -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_trickle';"
```

### Release Pipeline

- Push a tag → release workflow runs
- `publish-docker-arch` builds per-arch `scratch` images
- `publish-docker` creates multi-arch manifest at `ghcr.io/<owner>/pg_trickle-ext:<version>`
- `docker pull ghcr.io/<owner>/pg_trickle-ext:<version>` succeeds
- `docker inspect` confirms `scratch`-based image with OCI labels

---

## Rollback Plan

If the Image Volume approach is not viable (e.g., K8s 1.33 adoption is too slow):

1. Revert the Dockerfile changes (restore `cnpg/Dockerfile` and `cnpg/Dockerfile.release`)
2. Revert `IMAGE_NAME` in the release workflow
3. Revert `cluster-example.yaml`

All old files are preserved in git history. The release archive artifacts
(`.tar.gz` / `.zip`) are unaffected by this change and continue to work for
manual installation.

---

## ADR — Full Image vs Extension Image

### Context

The current approach bakes the pg_trickle extension into a full PostgreSQL
Docker image (`postgres:18.1` + extension files + UID remapping). This requires:

- Maintaining a custom PostgreSQL image
- Rebuilding on every PostgreSQL patch release
- UID/GID remapping for CNPG compatibility
- Users trust our image build instead of the official CNPG images

CloudNativePG 1.28 introduced Image Volume Extensions, which allow extensions
to be distributed as standalone OCI images mounted at runtime.

### Decision

Replace the full image with a `scratch`-based extension-only image.

### Consequences

**Positive:**
- Extension updates don't require a new PostgreSQL base image rebuild
- Users use official, hardened CNPG operand images
- Image is ~10 MB instead of ~400 MB
- No UID/GID remapping needed
- Supply chain is simpler (no OS packages to audit)

**Negative:**
- Requires Kubernetes 1.33 (not yet universally available)
- Requires CNPG 1.28+
- Users without K8s 1.33 must install from release archives or build a custom image themselves
- Local `docker run` workflow requires manual file mounting (no standalone image)
- CI smoke test complexity increases (kind may not support K8s 1.33 yet)

### Status

Accepted.

---

## Timeline

| Task | Effort | Dependencies |
|------|--------|-------------|
| Task 1 — Dockerfile.ext | 30 min | — |
| Task 2 — Dockerfile.ext-build | 1 hr | — |
| Task 3 — Remove old Dockerfiles | 10 min | Tasks 1–2 |
| Task 4 — Update release workflow | 2 hr | Task 1 |
| Task 5 — Update CI smoke test | 3 hr | Task 2, kind K8s 1.33 support |
| Task 6 — Cluster example manifests | 1 hr | — |
| Task 7 — Update justfile | 10 min | Task 2 |
| Task 8 — Update documentation | 2 hr | Tasks 1, 6 |
| Task 9 — Dependabot & path triggers | 15 min | Tasks 1–2 |
| **Total** | **~10 hours** | |

Tasks 1, 2, and 6 can be done in parallel. Tasks 4 and 8 follow once the
Dockerfiles are finalized. Task 5 may be deferred if kind doesn't support
K8s 1.33 yet.
