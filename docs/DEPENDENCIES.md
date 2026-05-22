# Dependency Policy

This document describes the criteria for adding, updating, and removing Rust
crate dependencies in `pg_trickle`.

---

## Guiding Principles

1. **Minimal surface area** — every dependency adds compile-time overhead,
   potential supply-chain risk, and maintenance burden. Prefer standard library
   or already-used crates before introducing a new one.

2. **Audit before adding** — new production (non-dev, non-test) dependencies
   must pass a `cargo deny check` audit (see `deny.toml`). Crates flagged for
   known advisories, unmaintained status, or license incompatibility are blocked.

3. **Pinned transitive critical paths** — direct dependencies used in the
   PostgreSQL extension shared library (anything linked via `pgrx`) are pinned
   to a minor version range (`~x.y`) in `Cargo.toml` to prevent unintentional
   major-version drift in CI.

4. **Feature-gated optional dependencies** — heavy or platform-specific crates
   (`object_store`, `aws-sdk-*`) should be optional features to keep the
   default binary small and to avoid build-time failures on platforms where the
   crate is not supported.

---

## Adding a New Dependency

1. Check `deny.toml` allows the license (`allow = [...]` list).
2. Run `cargo deny check` and fix any warnings before opening the PR.
3. Add a comment above the `[dependencies]` entry in `Cargo.toml` explaining
   why the crate is needed and which item ID (e.g. `ARCH-002`) drove the
   addition.
4. Prefer crates already used transitively over introducing a new root
   dependency.

---

## Updating Dependencies

- **Patch updates** (`x.y.z` → `x.y.z+1`): automatic via Dependabot / `cargo
  update`. No manual review required.
- **Minor updates** (`x.y` → `x.y+1`): allowed with a brief changelog review
  confirming no breaking behaviour changes.
- **Major updates** (`x` → `x+1`): require an explicit PR with a compatibility
  review and updated integration tests.

---

## Removing a Dependency

When a feature or workaround is replaced (e.g. `SCAL-001` removed `pool.rs`),
the associated crates should be removed from `Cargo.toml` in the same PR.
Run `cargo deny check` and `cargo test` after removal to confirm nothing else
depended on the removed crate.

---

## DuckLake Sink Dependencies

The DuckLake write path (introduced v0.66.0) uses:

| Crate | Role | Justification |
|-------|------|---------------|
| `arrow-array` | In-memory columnar representation | Required by `parquet` writer |
| `arrow-schema` | Schema type definitions | Required by `arrow-array` |
| `parquet` | Parquet file serialisation | Core sink format |
| `object_store` (optional feature `s3`) | S3/GCS object upload | Transport layer for cloud sinks |
| `bytes` | Zero-copy byte buffer | Used by `object_store` API |
| `tokio` (optional feature `rt`) | Async runtime for object_store | Isolated runtime per sink call |

These dependencies are reviewed each release. Any crate reaching end-of-life
or accumulating unpatched advisories will be replaced or removed at the
following release boundary.

---

## CI Enforcement

`cargo deny check` runs on every pull request targeting `main` (see
`.github/workflows/ci.yml`, job `cargo-deny`). A failing `deny` check blocks
merge.
