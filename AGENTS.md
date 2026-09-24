# Development guidelines for pg_trickle

PostgreSQL 18 extension in Rust with pgrx 0.18.x, implementing stream tables
through differential view maintenance.

Optimize for low latency, high throughput, and scalability. Use differential
refresh wherever possible. Full refresh is a last resort. Never trade correctness
or durability of committed changes for performance. Data loss is unacceptable.

## Workflow

- After any code change, run `just fmt`, then `just lint`. Both must pass with
  zero warnings. The [justfile](justfile) defines the checks and test recipes.
- After SQL-facing changes, run the relevant tiers under [Testing](#testing).
- Create a new branch only when the current branch is `main`.
- Commit only with explicit user authorization. When you edit files, include
  staging and commit commands in the final response, with a message summarizing
  the changes. Separate unrelated changes into separate commits when useful.

### Pull requests

For PR creation or description updates:

1. Remove any stale temporary body file, for example `rm -f /tmp/pr_TICKETNAME.md`.
2. Write the UTF-8 description with the `create_file` tool. Do not use shell
   heredocs or `echo` to write PR bodies, to avoid corrupted or stale content.
3. Read the file back. Verify line breaks, Unicode, and the absence of garbled
   content before using it.
4. Pass the file to `gh pr create --title "..." --body-file /tmp/pr_TICKETNAME.md`
   or `gh pr edit <number> --body-file /tmp/pr_TICKETNAME.md`.
5. Verify the published body with `gh pr view <number> --json body --jq '.body'`.

## Coding conventions

### Errors and SQL boundaries

- Define errors as `PgTrickleError` variants in [src/error.rs](src/error.rs).
  Propagate `Result<T, PgTrickleError>` and convert at the API boundary with
  `pgrx::error!()` or `ereport!()`.
- Keep `unwrap()` and `panic!()` out of non-test code, including SQL-reachable code.
- Include context in errors, such as the table name or query fragment.
- Annotate SQL functions with `#[pg_extern(schema = "pgtrickle")]`.
- Keep catalog tables in `pgtrickle` and change buffers in `pgtrickle_changes`.
- Use pgrx logging macros such as `pgrx::log!()`, `info!()`, `warning!()`, and
  `error!()`. Do not use `println!()` or `eprintln!()`.

### SPI

- Access catalogs through `Spi::connect()`. Keep connections short-lived and
  move long operations outside SPI blocks.
- Cast PostgreSQL `name` columns to `text` before fetching Rust `String` values,
  for example `n.nspname::text`. `name` has OID 19; pgrx expects `text`, OID 25.
- Return `Option` for missing catalog objects. CTEs, subquery aliases, and
  function-call ranges need not exist in `pg_class`. Avoid `.first().get()` on
  possibly empty results.
- Separate pure decision logic from SPI calls for unit testing without a
  backend. See `classify_relkind` and `strip_view_definition_suffix` for examples.

### Memory, workers, and configuration

- Minimize `unsafe` and wrap `pg_sys::*` calls in safe abstractions. Every
  `unsafe` block needs a `// SAFETY:` comment.
- Be explicit about PostgreSQL memory contexts. Use `PgLwLock` or `PgAtomic`
  for shared state and initialize it with `pg_shmem_init!()`.
- Register workers with `BackgroundWorkerBuilder`. Check `pg_trickle.enabled`
  before doing work and handle `SIGTERM` gracefully.
- Document GUCs and choose sensible defaults. Every
  `pub static PGS_*: GucSetting<...>` needs a `///` doc comment so
  `gen_catalogs.py` includes it in [docs/GUC_CATALOG.md](docs/GUC_CATALOG.md).

## Testing

Use the relevant recipe from the [justfile](justfile):

| Scope | Command | Infrastructure |
|-------|---------|----------------|
| Pure Rust unit tests in `src/` | `just test-unit` | No database |
| Integration tests in `tests/` | `just test-integration` | Testcontainers |
| Eligible E2E tests | `just test-light-e2e` | Packaged extension mounted into stock PostgreSQL |
| Full E2E tests | `just test-e2e` | Rebuilds the custom Docker image |
| Unit, integration, full E2E, and pgrx tests | `just test-all` | Docker and pgrx |
| Ignored TPC-H tests | `just test-tpch` | Rebuilds the custom Docker image |
| dbt integration | `just test-dbt` | Docker and dbt |

- Use Testcontainers for integration and E2E tests, never a local PostgreSQL
  instance. Use `#[tokio::test]`, name tests
  `test_<component>_<scenario>_<expected>`, and cover success and failure paths.
- Reuse [tests/common/mod.rs](tests/common/mod.rs). The custom image is defined
  in [tests/Dockerfile.e2e](tests/Dockerfile.e2e).
- Check [scripts/run_light_e2e_tests.sh](scripts/run_light_e2e_tests.sh) for light
  E2E eligibility. It packages the extension with `cargo pgrx package`.
- Rebuild stale E2E images with `just build-e2e-image` before using a runner that
  skips the build. `just test-dbt-fast` skips the dbt image rebuild.
- `just test-all` excludes ignored TPC-H tests. Use `TPCH_CYCLES` to control the
  TPC-H mutation cycle count, for example `TPCH_CYCLES=5 just test-tpch`.
- Before merging DVM, refresh, or CDC changes, a manual CI run is recommended:
  `gh workflow run ci.yml --ref <branch-name>`. Check
  [.github/workflows/](.github/workflows/) for current triggers and coverage.
  TPC-H nightly, benchmarks, and stability tests also have separate workflows.

## Task-specific references

- For architecture and module responsibilities, read
  [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and inspect [src/](src/).
- For SQL API changes, read [docs/SQL_REFERENCE.md](docs/SQL_REFERENCE.md).
- For GUC changes, read [docs/CONFIGURATION.md](docs/CONFIGURATION.md).
- For build prerequisites, read [INSTALL.md](INSTALL.md).
- For CDC changes, read [src/cdc/mod.rs](src/cdc/mod.rs),
  [src/config/cdc.rs](src/config/cdc.rs), and the decisions in
  [plans/adrs/PLAN_ADRS.md](plans/adrs/PLAN_ADRS.md). Default CDC uses
  statement-level AFTER triggers to capture changes into
  `pgtrickle_changes.changes_<oid>` in the source transaction. Preserve that
  atomicity when changing trigger-based capture.
- For design plans, read [plans/PLAN.md](plans/PLAN.md).
- For dbt work, follow [dbt-pgtrickle/AGENTS.md](dbt-pgtrickle/AGENTS.md).

## Agent skills

### Issue tracker

Issues and specs live in GitHub Issues; use `gh` for operations. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the five default triage labels. See `docs/agents/triage-labels.md`.

### Domain docs

Use the single-context layout. See `docs/agents/domain.md`.
