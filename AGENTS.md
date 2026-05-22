# AGENTS.md — Development Guidelines for pg_trickle

## Project Overview

PostgreSQL 18 extension written in Rust using **pgrx 0.18.x** that implements
streaming tables with incremental view maintenance (differential dataflow).
Targets PostgreSQL 18.x.

**Primary goals:** Maximum performance, low latency, and high throughput for
stream tables are the top priorities. Differential refresh mode must be used
wherever possible — full refresh is a fallback of last resort. The performance
and scalability of this extension should be world-class. Data loss is
unacceptable — correctness and durability of committed changes must never be
sacrificed for performance gains.

Key docs: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
[docs/SQL_REFERENCE.md](docs/SQL_REFERENCE.md) ·
[docs/CONFIGURATION.md](docs/CONFIGURATION.md) ·
[INSTALL.md](INSTALL.md)

---

## Workflow — Always Do This

After **any** code change:

```bash
just fmt          # Format code
just lint         # clippy + fmt-check (must pass with zero warnings)
```

After changes to SQL-facing code, run the relevant test tier:

```bash
just test-unit         # Pure Rust unit tests (no DB)
just test-integration  # Testcontainers-based integration tests
just test-e2e          # Full extension E2E tests (builds Docker image)
just test-all          # All of the above + pgrx tests
```

> E2E tests require a Docker image. Run `just build-e2e-image` if the image is
> stale, or use `just test-e2e` which rebuilds automatically.

When you're done and have edited files always remember to output git commands
for staging and committing the changes. The git commit message should summarize
the changes made. Feel free to put discrete changes into separate git commit
commands. Do not commit directly to git unless the user explicitly says it is
fine.

Never create a new git branch unless the current branch is `main`.

When creating a pull request, always write the PR description to a temporary
file using the **`create_file` tool** (never a shell heredoc or `echo`), then
pass it to `gh` via `--body-file`.  Shell heredocs and terminal commands
silently corrupt Unicode characters and can pick up stale content from a
previous session's file at the same path.

**Guaranteed-safe workflow:**

1. Delete any stale file at the target path first:
   ```bash
   rm -f /tmp/pr_TICKETNAME.md
   ```

2. Use the `create_file` tool to write the description.  The file is written
   in UTF-8 and read directly by `gh --body-file`, so Unicode characters
   (math symbols, em-dashes, etc.) are safe to use.

3. Verify the file is clean before using it:
   ```bash
   python3 -c "
   with open('/tmp/pr_TICKETNAME.md') as f:
       body = f.read()
   print('lines:', body.count(chr(10)))
   print('ok:', '####' not in body)
   print(body[:120])
   "
   ```

4. Create or update the PR:
   ```bash
   gh pr create --title "..." --body-file /tmp/pr_TICKETNAME.md
   # or, to fix a garbled description:
   gh pr edit <number> --body-file /tmp/pr_TICKETNAME.md
   ```

5. Verify the live PR body is not garbled:
   ```bash
   gh pr view <number> --json body --jq '.body' | head -20
   ```

---

## Coding Conventions

### Error Handling

- Define errors in `src/error.rs` as `PgTrickleError` enum variants.
- Never `unwrap()` or `panic!()` in code reachable from SQL.
- Propagate via `Result<T, PgTrickleError>`; convert at the API boundary with
  `pgrx::error!()` or `ereport!()`.

### SPI

- All catalog access via `Spi::connect()`.
- Keep SPI blocks short — no long operations while holding a connection.
- **Always cast `name`-typed columns to `text`** when fetching into Rust
  `String` (e.g. `n.nspname::text`). The PostgreSQL `name` type (Oid 19)
  is not compatible with pgrx `.get::<String>()` (Oid 25).
- Catalog lookups must handle "not found" gracefully — CTEs, subquery aliases,
  and function-call ranges do not exist in `pg_class`. Return `Option` rather
  than `.first().get()` which panics on empty results.
- **Separate pure logic from SPI calls** so the decision logic can be
  unit-tested without a PostgreSQL backend (see `classify_relkind`,
  `strip_view_definition_suffix` for examples).

### Unsafe Code

- Minimize `unsafe` blocks. Wrap `pg_sys::*` in safe abstractions.
- Every `unsafe` block must have a `// SAFETY:` comment.

### Memory & Shared State

- Be explicit about PostgreSQL memory contexts.
- Use `PgLwLock` / `PgAtomic` for shared state; initialize via `pg_shmem_init!()`.

### Background Workers

- Register via `BackgroundWorkerBuilder`.
- Check `pg_trickle.enabled` GUC before doing work.
- Handle `SIGTERM` gracefully.

### Logging

- Use `pgrx::log!()`, `info!()`, `warning!()`, `error!()`.
- Never `println!()` or `eprintln!()`.

### SQL Functions

- Annotate with `#[pg_extern(schema = "pgtrickle")]`.
- Catalog tables live in schema `pgtrickle`, change buffers in `pgtrickle_changes`.

---

## Module Layout

```
src/
├── lib.rs          # Extension entry point, GUCs, shmem init
├── api.rs          # SQL-callable functions (create/alter/drop/refresh)
├── catalog.rs      # pgtrickle.pgt_stream_tables CRUD
├── cdc.rs          # Change-data-capture (trigger-based)
├── config.rs       # GUC definitions
├── dag.rs          # Dependency graph, topological sort, cycle detection
├── error.rs        # PgTrickleError enum
├── hash.rs         # Content hashing for change detection
├── hooks.rs        # DDL event trigger hooks
├── monitor.rs      # Monitoring / metrics
├── refresh.rs      # Full + differential refresh orchestration
├── scheduler.rs    # Background worker scheduling
├── shmem.rs        # Shared memory structures
├── version.rs      # Extension version
├── wal_decoder.rs  # WAL-based CDC (logical replication polling, transitions)
└── dvm/            # Differential view maintenance engine
    ├── mod.rs
    ├── diff.rs     # Delta application
    ├── parser/     # Query analysis (modularized, G13-PRF)
    │   ├── mod.rs        # FFI helpers, macros, entry points, tests
    │   ├── types.rs      # OpTree, Expr, Column, AggExpr, etc.
    │   ├── validation.rs # Volatility, IVM support, IMMEDIATE, monotonicity
    │   ├── rewrites.rs   # SQL rewrite passes (view inlining, grouping sets, etc.)
    │   └── sublinks.rs   # SubLink extraction from WHERE clauses
    ├── row_id.rs   # Row identity tracking
    └── operators/  # Per-SQL-operator differentiation rules
```

See [plans/PLAN.md](plans/PLAN.md) for the full design plan.

---

## Testing

Six test tiers, each with its own infrastructure:

| Tier | Location | Runner | Needs DB? |
|------|----------|--------|----------|
| Unit | `src/**` (`#[cfg(test)]`) | `just test-unit` | No |
| Integration | `tests/*_tests.rs` (not `e2e_*`) | `just test-integration` | Yes (Testcontainers) |
| Light E2E | `tests/e2e_*_tests.rs` (most) | `just test-light-e2e` | Yes (stock postgres:18.3) |
| Full E2E | `tests/e2e_*_tests.rs` (10 files) | `just test-e2e` | Yes (custom Docker image) |
| TPC-H | `tests/e2e_tpch_tests.rs` (`#[ignore]`) | see below | Yes (custom Docker image) |
| dbt | `dbt-pgtrickle/integration_tests/` | `just test-dbt` | Yes (Docker + dbt) |

Light E2E uses `cargo pgrx package` output bind-mounted into a stock postgres
container (no custom Docker image build). 42 test files (~570 tests) are
light-eligible; 10 files (~90 tests) require the full E2E image.

- Shared helpers live in `tests/common/mod.rs`.
- E2E Docker images are built from `tests/Dockerfile.e2e`.
- Use `#[tokio::test]` for all integration/E2E tests.
- Name tests: `test_<component>_<scenario>_<expected>`.
- Test both success and failure paths.
- dbt tests use `just test-dbt-fast` to skip Docker image rebuild.

### CI Coverage by Trigger

| Job | PR | Push to main | Daily schedule | Manual dispatch |
|-----|-----|-------------|----------------|------------------|
| Unit (Linux) | ✅ | ✅ | ✅ | ✅ |
| Unit (macOS + Windows) | ❌ | ❌ | ✅ | ✅ |
| Integration | ✅ | ✅ | ✅ | ✅ |
| Light E2E | ✅ | ✅ | ✅ | ✅ |
| Full E2E + TPC-H | ❌ | ✅ | ✅ | ✅ |
| Upgrade completeness | ✅ | ✅ | ✅ | ✅ |
| Upgrade E2E | ❌ | ✅ | ✅ | ✅ |
| Benchmarks | ❌ | ✅ | ✅ | ✅ |
| Benchmark regression gate | ✅ | ❌ | ❌ | ✅ |
| DAG bench (calc/throughput) | ❌ | ❌ | ✅ | ✅ |
| DAG bench (parallel workers) | ❌ | ❌ | ✅ | ✅ |
| E2E bench — refresh matrix | ❌ | ❌ | Weekly (Sun) | ✅ |
| E2E bench — zero-change latency | ❌ | ❌ | Weekly (Sun) | ✅ |
| E2E bench — CDC overhead | ❌ | ❌ | Weekly (Sun) | ✅ |
| E2E bench — TPC-H FULL vs DIFF | ❌ | ❌ | Weekly (Sun) | ✅ |
| dbt integration | ❌ | ❌ | ✅ | ✅ |
| CNPG smoke test | ❌ | ❌ | ✅ | ✅ |
| Soak test (G17-SOAK)¹ | ❌ | ❌ | ✅ | ✅ |
| Multi-database (G17-MDB)¹ | ❌ | ❌ | ✅ | ✅ |

¹ Soak and multi-database tests run in the separate `stability-tests.yml`
workflow, not in ci.yml.

> **Note:** Full E2E and TPC-H tests are **skipped on PRs** (the Docker build
> is ~20 min). Light E2E tests run on every PR using `cargo pgrx package` +
> stock postgres container. Benchmark regression check runs on every PR targeting
> `main` using the Criterion baseline saved by the last push to `main`
> (via `scripts/criterion_regression_check.py`). To run the full CI matrix on a
> PR branch, use manual dispatch (see below).

### Running All Tests Locally

```bash
just test-all          # unit + integration + e2e (builds Docker image)

# TPC-H tests (not included in test-all, require e2e image):
just build-e2e-image
cargo test --test e2e_tpch_tests -- --ignored --test-threads=1 --nocapture

# Control TPC-H cycle count (default varies per test):
TPCH_CYCLES=5 cargo test --test e2e_tpch_tests -- --ignored --test-threads=1 --nocapture
```

### Triggering Full CI on a PR Branch

To run the complete CI matrix (including E2E, TPC-H, benchmarks, dbt, and
CNPG) on a feature branch:

```bash
gh workflow run ci.yml --ref <branch-name>
```

This is recommended before merging any PR that touches the DVM engine,
refresh logic, or CDC pipeline.

---

## CDC Architecture

The extension uses **row-level AFTER triggers** (not logical replication) to
capture changes into buffer tables (`pgtrickle_changes.changes_<oid>`). This
was chosen for single-transaction atomicity — see ADR-001 and ADR-002 in
[plans/adrs/PLAN_ADRS.md](plans/adrs/PLAN_ADRS.md) for the full rationale.

---

## Code Review Checklist

- [ ] No `unwrap()` / `panic!()` in non-test code
- [ ] All `unsafe` blocks have `// SAFETY:` comments
- [ ] SPI connections are short-lived
- [ ] New SQL functions use `#[pg_extern(schema = "pgtrickle")]`
- [ ] Tests use Testcontainers — never a local PG instance
- [ ] Error messages include context (table name, query fragment)
- [ ] GUC variables are documented with sensible defaults
- [ ] Background workers handle `SIGTERM` and check `pg_trickle.enabled`
- [ ] Every `pub static PGS_*: GucSetting<...>` has a `///` doc comment (required for `gen_catalogs.py` to include it in `docs/GUC_CATALOG.md`)
