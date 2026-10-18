# Contributing to pg_trickle

Thank you for your interest in contributing! pg_trickle is an Apache 2.0-licensed
open-source project and welcomes contributions of all kinds.

## Before You Start

- Check the [open issues](../../issues) and [discussions](../../discussions) to
  avoid duplicating work.
- For non-trivial changes, open an issue first to discuss the approach.
- Read [AGENTS.md](AGENTS.md) — it is the authoritative guide for all coding
  conventions, error handling rules, module layout, and test requirements.
- Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) to understand the system.
- Read [ROADMAP.md](ROADMAP.md) to see what work is planned.

## Ways to Contribute

| Type | Where to start |
|------|----------------|
| Bug report | [Open an issue](../../issues/new?template=bug_report.md) |
| Feature request | [Open an issue](../../issues/new?template=feature_request.md) or [start a discussion](../../discussions) |
| Documentation fix | Open a PR directly — no issue needed for typos/clarity |
| Code fix or feature | Open an issue first, then a PR |
| Performance improvement | Include benchmark numbers (see `just bench`) |

## Development Setup

```bash
# Install pgrx
cargo install cargo-pgrx --version "=0.18.0"
cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config

# Build
cargo build

# Format + lint (required before every PR)
# CI-004: `just lint` now includes docs-lint as the final step.
just fmt
just lint

# Run tests
just test-unit          # fast, no DB
just test-integration   # Testcontainers
just test-light-e2e     # PR-equivalent Light E2E tier (stock postgres)
just test-e2e           # full E2E (builds Docker image)
just test-pgbouncer     # PgBouncer transaction-pool compatibility tests
```

For differential correctness, prefer multi-cycle mutation tests that compare
the stream table with a fresh query result. Concurrency tests need a barrier or
an observed database state to prove operations overlapped. Poll for completion
instead of relying on fixed sleeps.

Full setup instructions are in [INSTALL.md](INSTALL.md).

### Devcontainer / Containerized Development

If you are developing in a devcontainer, use the default non-root `vscode` user
and run the normal commands from the workspace root:

```bash
just fmt
just lint
just test-unit
```

`just test-unit` uses `scripts/run_unit_tests.sh`, which now selects a writable
and cache-friendly target directory in this order:

1. `target/` (preferred)
2. `.cargo-target/` (project-local fallback)
3. `$HOME/.cache/pg_trickle-target`
4. `${TMPDIR:-/tmp}/pg_trickle-target` (last resort)

This avoids permission failures on bind mounts and preserves incremental builds
when source or test files change.

If you see permission errors in containerized runs, verify you are not forcing a
different container user/UID than expected by your workspace mount.

#### Run E2E tests in devcontainer

E2E tests use Testcontainers and require Docker access from inside the
devcontainer (provided by the Docker-in-Docker feature in
`.devcontainer/devcontainer.json`).

Run from the workspace root inside the devcontainer:

```bash
just build-e2e-image
just test-e2e
```

Notes:

- The E2E harness starts containers via `testcontainers` (`tests/e2e/mod.rs`).
- The default E2E image is `pg_trickle_e2e:latest` (built by
  `tests/build_e2e_image.sh`).
- A plain `docker run` of the dev image is not equivalent to a full VS Code
  devcontainer session with features/lifecycle hooks enabled.

## Making a Pull Request

1. Fork the repository and create a branch: `git checkout -b fix/my-fix`
2. Make your changes following the conventions in [AGENTS.md](AGENTS.md)
3. Run `just fmt && just lint` — both must pass with zero warnings
4. Add or update tests — see [AGENTS.md § Testing](AGENTS.md#testing)
5. Open a PR against `main`

The PR template will walk you through the checklist.

### CI Coverage on PRs

Every PR runs static/release contracts, script unit tests, upgrade completeness,
and the checked LSN proof. The contracts also classify changed files using
[.github/ci-paths.yml](.github/ci-paths.yml). Expensive CI jobs wait for the
contracts to pass and run only when the relevant inputs changed:

| Changed inputs | Selected work beyond the contracts and proof |
|----------------|---------------------------------------------|
| Rust, extension SQL, build inputs, test fixtures/harness | Unit/integration tests and the existing packaged/sensitive E2E gates |
| Documentation, release-contract data, ordinary maintenance scripts | Documentation checks and script tests; no extension rebuild |
| Benchmark sources or inputs | Benchmark tracking on main; measurements also remain nightly/manual |
| Shipping Dockerfile or PostgreSQL defaults | Shipping-image qualification |
| Monitoring configuration | Collector contract |
| dbt getting-started project or adapter | Getting-started example on main; full dbt integration remains scheduled/manual |
| Demo/CNPG examples | Static checks; CNPG deployment verification remains scheduled/manual |

Build inputs include `build.rs`, `.cargo/**`, `pg_trickle.control`, Cargo
manifests, and the scripts used by compiled tests. Unclassified paths retain
runtime coverage. A mixed PR selects the union of the relevant checks. The
standalone lint, benchmark-tracking, and coverage workflows also filter their
inputs, so a documentation-only merge does not rebuild Rust targets.

Draft PRs defer packaging, light/full E2E, shipping-image qualification,
Windows compilation, and fuzz corpus replay.
Marking a draft **ready for review** starts those jobs; converting it back to
a draft cancels the previous run and returns to the smaller set of checks.

Ready PRs with runtime changes additionally run packaged-extension coverage,
including light E2E split across three shards against stock `postgres:18.6`. Sensitive paths select
the existing full E2E, security, failure-recovery, or unsafe-boundary gates.

Criterion regression comparisons and the pgbench overhead gate run nightly
and remain available manually. Nightly Criterion runs measure both the
candidate and the last successful scheduled CI revision on the same runner;
the first nightly run falls back to the previous commit with a warning. The
compared commit IDs are recorded in the job summary. TPC-H, dbt, CNPG, and the
extra macOS/Windows unit jobs also retain their scheduled/manual coverage.

#### Workflow trigger matrix

PR and push entries below are subject to the input selection described above.
Nightly and manual CI runs retain the full matrix regardless of changed paths.

| Workflow / Job | PR | Push to main | Daily schedule | Manual dispatch |
|----------------|----|--------------|----------------|-----------------|
| Unit tests (Linux) | ✅ | ✅ | ✅ | ✅ |
| Unit tests (macOS + Windows) | ❌ | ❌ | ✅ | ✅ |
| Integration tests | ✅ | ✅ | ✅ | ✅ |
| Light E2E (3 shards) | Ready only | ✅ | ✅ | ✅ |
| Full E2E | Sensitive paths, ready only | ❌ | ✅ | ✅ |
| TPC-H | ❌ | ❌ | ✅ | ✅ |
| Upgrade completeness check | ✅ | ✅ | ✅ | ✅ |
| Upgrade E2E | ❌ | ❌ | ✅ | ✅ |
| Benchmarks | ❌ | ✅ | ✅ | ✅ |
| Benchmark regression gate | ❌ | ❌ | ✅ | Opt-in |
| pgbench overhead gate | ❌ | ❌ | ✅ | ✅ |
| Shipping image upgrade | Ready only | Shipping inputs | ✅ | ✅ |
| DAG bench (calc / throughput) | ❌ | ❌ | ✅ | ✅ |
| DAG bench (parallel workers) | ❌ | ❌ | ✅ | ✅ |
| E2E bench — refresh matrix | ❌ | ❌ | Weekly (Sun) | ✅ |
| E2E bench — zero-change latency | ❌ | ❌ | Weekly (Sun) | ✅ |
| E2E bench — CDC overhead | ❌ | ❌ | Weekly (Sun) | ✅ |
| E2E bench — TPC-H FULL vs DIFF | ❌ | ❌ | Weekly (Sun) | ✅ |
| dbt integration | ❌ | ❌ | ✅ | ✅ |
| CNPG smoke test | ❌ | ❌ | ✅ | ✅ |
| Soak test (stability-tests.yml) | ❌ | ❌ | ✅ | ✅ |
| Multi-database (stability-tests.yml) | ❌ | ❌ | ✅ | ✅ |
| Lint (fmt + clippy + docs-lint) | ✅ | ✅ | ✅ | ✅ |
| Unsafe block inventory | ✅ | ✅ | ✅ | ✅ |
| GUC + SQL API catalog drift (O40-1) | ✅ | ✅ | ✅ | ✅ |
| cargo-deny (dependency policy) | ✅¹ | ✅ | ✅ | ✅ |
| cargo audit (security) | ✅ | ✅ | ✅ | ✅ |
| Semgrep static analysis | ✅ | ✅ | ✅ | ✅ |
| Secret scanning (gitleaks) | ✅ | ✅ | ✅ | ✅ |
| Fuzz corpus replay | Ready only | ❌ | ✅ | ✅ |
| Windows compile check | Ready only | ❌ | ❌ | ❌ |

¹ `cargo-deny` runs on PRs only when `Cargo.toml`, `Cargo.lock`, or `deny.toml`
change (path filter).  If your PR does not touch these files, the check is skipped
but not blocking.

To trigger the **full CI matrix** on your PR branch (recommended for DVM
engine, refresh, or CDC changes):

```bash
gh workflow run ci.yml --ref <your-branch>
```

Manual dispatch also runs the heavy tests on a draft branch. To opt in to the
Criterion comparison against an exact base ref or commit:

```bash
gh workflow run ci.yml --ref <your-branch> \
  -f run_benchmark_regression=true -f benchmark_base=<base-ref-or-sha>
```

Omitting `benchmark_base` on manual runs compares against `main`.

To run all tests locally before pushing:

```bash
just test-all          # unit + integration + e2e

# PR-equivalent fast path:
just test-unit
just test-integration
just test-light-e2e

# TPC-H correctness tests (requires e2e Docker image):
cargo test --test e2e_tpch_tests -- --ignored --test-threads=1 --nocapture
```

See [AGENTS.md § Testing](AGENTS.md#testing) for the full CI coverage matrix.

## Coding Conventions (summary)

- No `unwrap()` or `panic!()` in non-test code
- All `unsafe` blocks require a `// SAFETY:` comment
- Errors go through `PgTrickleError` in `src/error.rs`
- New SQL functions use `#[pg_extern(schema = "pgtrickle")]`
- Tests use Testcontainers — never a local PostgreSQL instance

Full details are in [AGENTS.md](AGENTS.md).

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
fix: correct pgoutput action parsing for tables named INSERT_LOG
feat: add CUBE explosion guard (max 64 UNION ALL branches)
docs: document JOIN key change limitation in SQL_REFERENCE
test: add E2E test for keyless table duplicate-row behaviour
```

## Fuzz Testing

pg_trickle uses `cargo-fuzz` (libFuzzer) to exercise core parser and pipeline
logic. Fuzz targets live in `fuzz/fuzz_targets/`.

### Available targets

| Target | What it exercises | Added in |
|--------|-------------------|----------|
| `parser_fuzz` | DVM SQL parser (OpTree construction) | v0.1.0 |
| `cron_fuzz` | Cron expression parser | v0.26.0 |
| `guc_fuzz` | GUC string→enum coercion | v0.26.0 |
| `cdc_fuzz` | CDC trigger payload decoding | v0.26.0 |
| `wal_fuzz` | WAL/SQLSTATE error classifier | v0.39.0 |
| `dag_fuzz` | DAG/merge SQL and snapshot column-list | v0.39.0 |
| `sql_builder_fuzz` | SQL builder + typed parser facade | v0.44.0 |
| `merge_sql_fuzz` | Merge SQL codegen with random change streams | v0.49.0 |
| `row_id_fuzz` | Row identity tracking with random operator trees | v0.49.0 |

### Running all fuzz targets

```bash
# Run each target for 60 s (requires nightly toolchain):
just fuzz-all

# Run for a custom duration (seconds):
just fuzz-all 120

# Run a single target:
cargo +nightly fuzz run merge_sql_fuzz -- -max_total_time=60
```

Corpus directories are at `fuzz/corpus/<target_name>/`. Regression cases are
stored in `proptest-regressions/`.



**Every new feature ships with documentation.** This is a hard requirement,
not a nice-to-have.

- New SQL functions → entry in `docs/SQL_REFERENCE.md`.
- New GUC variable → entry in `docs/CONFIGURATION.md`.
- New user-facing capability → at minimum a paragraph in the relevant
  chapter; for headline features, a dedicated page.
- New page → add it to `docs/SUMMARY.md` in the correct chapter.

PRs that introduce a `#[pg_extern]` function or a new GUC without
documentation will be asked to add it before merge.

## Updating base image digests

Docker base images are pinned to exact SHA256 digests in all Dockerfiles
(`Dockerfile.demo`, `Dockerfile.ghcr`, `tests/Dockerfile.e2e`,
`tests/Dockerfile.e2e-asan`) for
reproducibility and supply-chain security (OPS-10-03).

To update the digests when a new PostgreSQL patch release is available:

```bash
# Requires docker with manifest support
scripts/update_base_image_digests.sh
```

The script resolves the current `linux/amd64` digest for
`postgres:18.6-bookworm`, patches all Dockerfiles in-place, and prints the
commit command. Run this script **quarterly** or when a PostgreSQL patch release
is needed. Include the digest-update commit in the release PR.

If you are building for `linux/arm64` or another platform, edit the
`TARGET_PLATFORM` variable in the script or pin to the manifest index digest
(returned by `docker manifest inspect postgres:18.6-bookworm --verbose`).

## License

By contributing you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
