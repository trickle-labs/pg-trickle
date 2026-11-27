# Contributing to pg_trickle

Thanks for your interest in improving `pg_trickle`. We appreciate bug reports,
minimal SQL reproductions, root-cause investigations, benchmarks, documentation
fixes, and real-world usage feedback.

`pg_trickle` uses an **issue-first contribution model**. Start with a
well-written GitHub issue so we can understand the problem, discuss the desired
behavior, and agree on the right direction before anyone spends significant time
writing code.

**Please wait for an explicit maintainer invitation before opening a pull
request.**

> **Quick navigation:**
> - **Reporting a bug, requesting a feature, or asking a question?** Read
>   [Ways to contribute](#ways-to-contribute) through
>   [Licensing](#licensing).
> - **Invited to open a pull request (or working as a maintainer)?** Jump to
>   [Before opening an invited pull request](#before-opening-an-invited-pull-request).

## Ways to contribute

| Type | Where to start |
|------|----------------|
| Bug report | [Open a bug report](https://github.com/trickle-labs/pg-trickle/issues/new?template=bug_report.md) with a minimal SQL reproduction |
| Feature request or improvement | [Open a feature request](https://github.com/trickle-labs/pg-trickle/issues/new?template=feature_request.md) describing the problem and use case |
| Question or early idea | [Start a discussion](https://github.com/trickle-labs/pg-trickle/discussions) |
| Documentation fix or gap | Use the [bug report template](https://github.com/trickle-labs/pg-trickle/issues/new?template=bug_report.md) for inaccuracies, typos, or broken links, or the [feature request template](https://github.com/trickle-labs/pg-trickle/issues/new?template=feature_request.md) for missing guides |
| Code or performance change | Open an issue first and wait for a maintainer invitation before opening a PR |
| Security vulnerability | Report privately via [Security → Report a vulnerability](https://github.com/trickle-labs/pg-trickle/security/advisories/new) (see [SECURITY.md](SECURITY.md)) |

## Why we work this way

Writing code is only one part of maintaining a PostgreSQL extension. Before
making a change, we need to understand what problem is being solved, whether it
fits `pg_trickle`'s goals, how it affects differential maintenance correctness
and durability of committed changes, what upgrade or compatibility costs it
introduces, and whether there is a simpler design.

Starting with an issue builds shared understanding first and avoids wasted work
on implementations that do not fit the project's direction or invariants.

You do not need to write code for your contribution to matter. Some of the most
valuable contributions are:

- minimal SQL reproductions of bugs or differential-vs-full mismatches;
- failing test cases that demonstrate a correctness or concurrency issue;
- compatibility reports (PostgreSQL extensions, connection poolers, operators,
  platforms);
- investigations that narrow down the root cause of a failure;
- measurements and benchmarks on realistic workloads;
- SQL API, GUC, and error-message usability feedback;
- descriptions of real-world schemas and streaming workloads;
- documentation gaps, inaccuracies, and broken links; and
- careful testing of proposed fixes or pre-releases.

## Reporting a bug

Search [existing issues](https://github.com/trickle-labs/pg-trickle/issues) and
[discussions](https://github.com/trickle-labs/pg-trickle/discussions) before
opening a new one, as the problem may already have been reported or discussed.

Use the [bug report template](https://github.com/trickle-labs/pg-trickle/issues/new?template=bug_report.md)
and include whichever of the following are relevant:

- a clear description of what happened and what you expected instead;
- a minimal SQL reproduction (schema, defining query, DML sequence, and refresh
  steps);
- the `pg_trickle` version and PostgreSQL version;
- your OS / platform and deployment setup (bare metal, Docker, CNPG, etc.);
- the CDC mode (`trigger`, `auto`, or `wal`) and refresh mode (`AUTO`,
  `DIFFERENTIAL`, `FULL`, or `IMMEDIATE`);
- relevant GUC settings, PostgreSQL logs, stack traces, or error messages; and
- any concurrent activity or schema changes involved.

Minimal SQL reproductions are especially valuable. If the clearest way to
demonstrate a bug is a small SQL script showing a mismatch between a stream
table and a fresh execution of its defining query, paste it directly in the
issue. You do not need to open a pull request or know the root cause before
reporting a bug.

## Suggesting an improvement

Before opening a proposal, check [ROADMAP.md](ROADMAP.md),
[existing issues](https://github.com/trickle-labs/pg-trickle/issues), and
[discussions](https://github.com/trickle-labs/pg-trickle/discussions) to see if
the topic is already planned or under discussion.

Use the [feature request template](https://github.com/trickle-labs/pg-trickle/issues/new?template=feature_request.md)
and start from the problem or use case rather than a specific implementation.
It helps to describe:

- what you are trying to accomplish;
- why the current behavior is difficult, slow, or limiting;
- who benefits from the change and in what kind of workload;
- an example of the SQL API or behavior you have in mind;
- any workarounds you rely on today; and
- compatibility, upgrade, or correctness constraints you already know about.

You are welcome to share thoughts on a possible approach, but you do not need
to design the implementation. Maintainers may prefer a different approach after
weighing DVM semantics, CDC atomicity, upgrade compatibility, maintenance cost,
or the broader roadmap.

## From issue to pull request

An open issue is an invitation to discuss the problem, add context, investigate
the cause, or evaluate trade-offs. It is **not automatically an invitation to
begin implementing a change**, and we generally do not review unsolicited pull
requests.

A pull request is appropriate once:

1. the underlying problem or proposed change has been discussed in an issue;
2. maintainers and contributor have agreed on the general direction; and
3. a maintainer has explicitly invited an implementation.

If you would like to implement an issue, say so in the issue thread. We may
also use labels such as `help wanted` or `good first issue` to signal that an
issue is open to external contributors.

We aim to review new issues within about two weeks, though maintainer
availability varies and this is not a guaranteed response time. If an issue has
not received a maintainer response after that period, it is reasonable to
assume it is not a current priority—feel free to use a workaround, solve the
problem in a fork, or return later if you have new evidence. We state this up
front so contributors are not left waiting indefinitely or compelled to send
repeated status pings.

Pull requests opened without prior agreement may be closed without a full code
review. This is not a judgment on the quality of your work—it simply means the
project has not agreed to take on that change or spend maintainer time
reviewing its implementation.

### Small fixes and documentation

The issue-first approach applies to small fixes and documentation as well. Even
seemingly minor edits can interact with generated docs, catalog drift checks,
upgrade scripts, or broader project conventions, and keeping one contribution
path makes expectations clear for everyone.

For small documentation problems—such as a typo, outdated GUC name, or broken
link—reporting the file, location, and suggested correction in an issue is
already a complete contribution. A maintainer may fix it directly; there is no
need to open a pull request just to make the contribution count.

For larger documentation changes, open an issue describing what is unclear,
incorrect, or missing so we can agree on what the documentation should say
before anyone rewrites a section.

## AI-assisted contributions

AI tools can be helpful for investigating bugs, building minimal SQL
reproductions, exploring unfamiliar parts of the codebase, or working on an
invited implementation. Using AI tools is not a problem in itself, but **you
remain responsible for everything you submit**.

Please make sure that you:

- verify that reported behavior is real against an actual `pg_trickle` build;
- run and verify reproduction SQL yourself;
- check every technical claim, SQL function name, and GUC against the real
  codebase;
- understand every line of code you submit;
- run the local checks and tests before asking maintainers to review; and
- can discuss design trade-offs, respond to review feedback, and debug issues
  in your submission.

Submissions containing fabricated SQL APIs or GUCs, unverified claims,
irrelevant generated prose, or code the author cannot explain may be closed
without further review. High-volume or speculative submissions that create
disproportionate triage work may also be closed.

## Questions and early ideas

Use [GitHub Discussions](https://github.com/trickle-labs/pg-trickle/discussions)
for general questions, usage help, early-stage ideas, and architecture or
design exploration that is not yet concrete enough for an actionable issue.
Once a discussion narrows down to a concrete bug or a well-defined proposal,
move it into an issue.

## Security vulnerabilities

**Please do not report security vulnerabilities in public issues, discussions,
or pull requests.**

Use GitHub's private vulnerability reporting instead: go to
[Security → Report a vulnerability](https://github.com/trickle-labs/pg-trickle/security/advisories/new).
See [SECURITY.md](SECURITY.md) for scope, response targets, and what to
include.

## Maintainer decisions

Not every valid issue will result in a change to `pg_trickle`. Maintainers may
decline or defer a proposal because:

- it falls outside the project's scope;
- it weakens differential correctness or durability guarantees;
- the ongoing maintenance or upgrade-path cost is too high;
- the use case is very specialized;
- the goal can already be achieved another way;
- it conflicts with PostgreSQL or extension compatibility requirements; or
- it is not a current priority.

These decisions reflect the project's constraints, not the value of the person
who raised the issue. A clear report still informs future design even when we
choose not to act on it.

We may also close issues that cannot be reproduced, lack enough information to
investigate, have become inactive, or no longer apply to current versions. If
new evidence or a fresh reproduction turns up later, feel free to open a new
issue or ask whether an existing one should be revisited.

## Community expectations

Please keep discussions constructive, patient, and focused on the technical
problem. Maintainers may not be able to respond immediately, and contributors
bring different levels of familiarity with PostgreSQL internals and Rust.

To help keep the project manageable, please avoid:

- repeatedly asking for status updates;
- opening duplicate issues to draw attention;
- opening a pull request after being asked to wait;
- tagging individual maintainers unnecessarily; and
- privately contacting maintainers about ordinary project issues.

Keeping technical discussions in the project's public channels makes the
context searchable and useful to other users and future contributors.

### A simple rule of thumb

- If you find a problem, **show us the problem**.
- If you have an idea, **explain the use case**.
- If you know how you would solve it, **share your thinking in the issue**.
- If outside implementation would be helpful, **we will invite you to open a
  pull request**.

## Licensing

By submitting a contribution to `pg_trickle`, you agree to license that
contribution under the [Apache License 2.0](LICENSE), unless a separate
contributor agreement explicitly applies.

This applies to code, tests, SQL reproductions, documentation, examples, and
other material submitted for inclusion in the project. Please only submit
material that you have the right to contribute under those terms.

---

## Before opening an invited pull request

Everything below applies once a maintainer has invited you to work on a pull
request (or for maintainers working directly on the repository).

Before writing code, read:

- [AGENTS.md](AGENTS.md) — authoritative guide for coding conventions, error
  handling, SPI rules, memory/worker safety, and test requirements;
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — system architecture and module
  responsibilities; and
- [INSTALL.md](INSTALL.md) — full build and PostgreSQL setup prerequisites.

### Invited PR workflow

1. Fork the repository and create a branch from `main`:
   `git checkout -b fix/my-fix`
2. Keep changes focused on the agreed issue—avoid unrelated refactoring,
   cleanup, formatting churn, or extra features. If the scope changes
   significantly while you are working, pause and check back on the issue
   before continuing.
3. Follow [AGENTS.md](AGENTS.md) and add or update tests covering both success
   and failure paths (see [AGENTS.md § Testing](AGENTS.md#testing)).
4. Update documentation and [CHANGELOG.md](CHANGELOG.md) for any user-visible
   change (see [Documentation requirements](#documentation-requirements)).
5. Run the repository validation commands locally:
   - Formatting: `just fmt` (verify no formatting diff remains).
   - Core lint suite: `just lint` (format check, `clippy` with warnings as
     errors, `SECURITY DEFINER` and privilege-boundary checks, `docs-lint`,
     SQL-builder check, fuzz inventory check, and monitoring contract) — or
     `just lint-ci` to also run version-sync, stale-version, and capability
     manifest checks.
   - Targeted CI guards when applicable: `just check-docs-drift` (after GUC or
     SQL API changes), `just check-upgrade-all` (after catalog or SQL surface
     changes), `just unsafe-inventory` (after `unsafe` changes), and
     `just security` (after dependency changes).
   - Relevant test tiers: `just test-unit`, `just test-integration`,
     `just test-dvm`, `just test-light-e2e`, or `just test-e2e`.
6. Open a PR against `main`, reference the issue (`Fixes #<number>`), explain
   any non-obvious design choices or differences from the issue discussion, and
   complete the PR template checklist.

Use the repository's documented `just` commands rather than ad-hoc equivalents.
If you cannot run one of the checks in your environment (for example, Docker is
unavailable for Testcontainers), state that clearly in the PR description so
reviewers know what has and has not been validated.

### Development setup

Local development requires PostgreSQL 18, Rust 1.85+ (edition 2024),
`cargo-pgrx` `0.18.0`, [`just`](https://github.com/casey/just),
[`cargo-nextest`](https://nexte.st/) (used by `just test-integration` and E2E
runners), `python3`, and Docker (for Testcontainers and E2E tests).

```bash
# Install pgrx and nextest
cargo install --locked cargo-pgrx --version 0.18.0
cargo install --locked cargo-nextest
cargo pgrx init --pg18 "$(pg_config --bindir)/pg_config"

# Build
just build

# Format + lint (required before every PR; includes docs-lint)
just fmt
just lint

# Run tests
just test-unit          # fast, no DB (automatically preloads pg_stub)
just test-integration   # Testcontainers
just test-dvm           # generated DVM SQL against Testcontainers PostgreSQL
just test-light-e2e     # packaged extension mounted in stock postgres
just test-e2e           # full E2E (builds Docker image)
just test-pgbouncer     # PgBouncer transaction-pool compatibility tests
```

Always use `just test-unit` rather than bare `cargo test --lib`: the wrapper
automatically builds and preloads `scripts/pg_stub.c` so pure-Rust unit tests
run cleanly without a PostgreSQL process (especially important on macOS 26+ and
strict-linking Linux toolchains; see [INSTALL.md § Troubleshooting](INSTALL.md#troubleshooting)).

For differential correctness, prefer multi-cycle mutation tests that compare
the stream table with a fresh query result. Concurrency tests need a barrier or
an observed database state to prove operations overlapped. Poll for completion
instead of relying on fixed sleeps.

Full setup instructions (including macOS troubleshooting) are in
[INSTALL.md](INSTALL.md).

### Devcontainer / containerized development

If you are developing in the VS Code devcontainer (`.devcontainer/devcontainer.json`),
use the default non-root `vscode` user and run the normal `just` commands from
the workspace root:

```bash
just fmt
just lint
just test-unit
```

`scripts/run_unit_tests.sh` automatically falls back to a writable target
directory if `target/` is not writable on a bind mount. If you still see
permission errors in containerized runs, verify you are not overriding the
container user/UID expected by your workspace mount.

E2E tests require Docker access from inside the devcontainer (provided by the
Docker-in-Docker feature in `.devcontainer/devcontainer.json`):

```bash
just build-e2e-image
just test-e2e
```

A plain `docker run` of the dev image is not equivalent to a full devcontainer
session with features and lifecycle hooks enabled.

### Coding conventions (summary)

- No `unwrap()` or `panic!()` in non-test code.
- All `unsafe` blocks require a `// SAFETY:` comment. Adding a justified
  `unsafe` block also requires updating `.unsafe-baseline` via
  `scripts/unsafe_inventory.sh --update > .unsafe-baseline` (enforced by CI).
- Errors go through `PgTrickleError` in `src/error.rs` with relevant context.
- New SQL functions use `#[pg_extern(schema = "pgtrickle")]`, and catalog or
  SQL surface changes must be reflected in the corresponding upgrade migration
  script (`sql/pg_trickle--<from>--<to>.sql`, checked by `just check-upgrade-all`).
- Integration and E2E tests use Testcontainers — never a local PostgreSQL
  instance.

Full details are in [AGENTS.md](AGENTS.md).

### Documentation requirements

**Every new feature ships with documentation.** This is a hard requirement, not
a nice-to-have. See [docs/DOCS_OWNERSHIP.md](docs/DOCS_OWNERSHIP.md) for the
canonical source of each documentation surface.

- New SQL functions → entry in [docs/SQL_REFERENCE.md](docs/SQL_REFERENCE.md)
  and regenerate [docs/SQL_API_CATALOG.md](docs/SQL_API_CATALOG.md) via
  `just gen-catalogs`.
- New GUC variable → entry in [docs/CONFIGURATION.md](docs/CONFIGURATION.md)
  plus a `///` doc comment on `PGS_*`, then run `just gen-catalogs` to update
  [docs/GUC_CATALOG.md](docs/GUC_CATALOG.md).
- New user-facing capability → at minimum a paragraph in the relevant chapter
  and an entry under `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md); for
  headline features, add a dedicated page.
- New page → add it to [docs/SUMMARY.md](docs/SUMMARY.md) in the correct
  chapter.

Before opening or updating a PR with doc, GUC, or SQL API changes, run the
documentation drift guards:

```bash
just check-docs-drift               # verify GUC_CATALOG.md and SQL_API_CATALOG.md
python3 scripts/check_docs_truth.py # verify GUC and SQL function references in docs/
python3 check_links.py              # check internal Markdown links in docs/
```

PRs that introduce a `#[pg_extern]` function or a new GUC without documentation
will be asked to add it before merge.

### Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
fix: correct pgoutput action parsing for tables named INSERT_LOG
feat: add CUBE explosion guard (max 64 UNION ALL branches)
docs: document JOIN key change limitation in SQL_REFERENCE
test: add E2E test for keyless table duplicate-row behaviour
```

## CI coverage on PRs

Every PR runs static and release contracts, script unit tests, upgrade
completeness, and the pinned formal-verification proof suites. The contracts
also classify changed files using [.github/ci-paths.yml](.github/ci-paths.yml)
so expensive jobs wait for contracts to pass and run only when relevant inputs
changed:

| Changed inputs | Selected work beyond the contracts and proofs |
|----------------|-----------------------------------------------|
| Rust, extension SQL, build inputs, test fixtures/harness | Unit/catalog/DVM component tests; ready PRs add packaged and bounded full E2E |
| Documentation, release-contract data, ordinary maintenance scripts | Documentation checks and script tests; no extension rebuild |
| Benchmark sources or inputs | Benchmark tracking on `main`; measurements also remain nightly/manual |
| Shipping Dockerfile or PostgreSQL defaults | Shipping-image qualification |
| Monitoring configuration | OpenTelemetry Collector contract |
| dbt getting-started project or adapter | Getting-started example on `main`; full dbt integration remains scheduled/manual |
| Demo/CNPG examples | Static checks; CNPG deployment verification remains scheduled/manual |

Build inputs include `build.rs`, `.cargo/**`, `pg_trickle.control`, Cargo
manifests, and the scripts used by compiled tests. Unclassified paths retain
runtime coverage, and mixed PRs select the union of matching checks. Standalone
workflows—lint, docs drift, unsafe inventory, Semgrep, `cargo-deny`,
`cargo audit`, secret scanning, stability soak regressions, and fuzz corpus
replay—use their own path filters so documentation-only changes do not rebuild
Rust targets.

Draft PRs defer packaging, light/full E2E, shipping-image qualification,
Windows compilation, and fuzz corpus replay. Marking a draft **ready for
review** starts those jobs; converting it back to a draft cancels the previous
run and returns to the smaller set of checks.

Ready runtime PRs run the three-shard light-E2E suite against stock
`postgres:18.6` plus the bounded full-harness suite defined in
[.github/ci-suites.json](.github/ci-suites.json) with automatic test retries
disabled—investigate failures before rerunning. Refresh, CDC, and scheduler
changes add extra failure-recovery repetitions, and unsafe-boundary changes run
AddressSanitizer qualification. DVM component tests run in the integration job
using the `pg_stub`/Testcontainers runner.

Heavier suites—full E2E inventory, TPC-H, upgrade E2E, Criterion/pgbench/DAG/E2E
benchmarks, dbt integration, CNPG smoke, soak/multi-DB stability, extended
fuzzing, SQLancer, and macOS/Windows unit tests—run on scheduled and manual
workflows. For the full test-layer matrix, deferred-suite schedule, failure
ownership, and `PR correctness` aggregation rules, see
[docs/CI_COVERAGE.md](docs/CI_COVERAGE.md) and
[.github/workflows/](.github/workflows/).

### Triggering full CI and running checks locally

To trigger the **full CI matrix** on your PR branch (recommended for DVM
engine, refresh, or CDC changes):

```bash
gh workflow run ci.yml --ref <your-branch>
```

Manual dispatch also runs the heavy tests on a draft branch. To opt in to the
Criterion comparison against an exact base ref or commit (defaults to `main`
when `benchmark_base` is omitted):

```bash
gh workflow run ci.yml --ref <your-branch> \
  -f run_benchmark_regression=true -f benchmark_base=<base-ref-or-sha>
```

To run tests locally before pushing:

```bash
just test-all          # unit + integration + e2e + pgrx (excludes test-dvm and ignored TPC-H)

# PR-equivalent fast path:
just test-unit
just test-integration
just test-dvm
just test-light-e2e
just build-e2e-image
python3 scripts/ci_pr_suite.py

# Validate the inventory and gate logic without Docker:
python3 scripts/ci_pr_suite.py --list
python3 -m unittest discover -s scripts -p 'test_ci*.py'

# TPC-H correctness tests (requires Docker):
just test-tpch         # full rebuild + TPC-H suite
just test-tpch-fast    # reduced cycles, skips image rebuild
```

## Fuzz testing

`pg_trickle` uses `cargo-fuzz` (libFuzzer) with a nightly Rust toolchain
(`rustup install nightly` and `cargo install cargo-fuzz`) to exercise core
parser, decoder, SQL builder, DAG, merge, and CDC logic. Fuzz targets are
defined in `fuzz/Cargo.toml` under `fuzz/fuzz_targets/`.

```bash
# List all available fuzz targets:
python3 scripts/fuzz_targets.py

# Run every target for 60 s (requires nightly toolchain):
just fuzz-all

# Run for a custom duration (seconds):
just fuzz-all 120

# Run a single target:
cargo +nightly fuzz run merge_sql_fuzz -- -max_total_time=60
```

Corpus directories are stored under `fuzz/corpus/<target_name>/`, and committed
`proptest` regression cases live in `proptest-regressions/`.

## Maintainer tasks

> External contributors normally do not need to run the tasks in this section.

### Updating base image digests

Docker base images are pinned to exact SHA256 manifest-list digests in
`Dockerfile.demo`, `Dockerfile.ghcr`, `tests/Dockerfile.e2e`, and
`tests/Dockerfile.e2e-asan` for reproducibility and supply-chain security. The
full E2E image uses PostgreSQL 18.7 because the current `pgvector` package
requires that patch version; the other images use 18.6.

To update the digests quarterly or when a new PostgreSQL patch release is
available:

```bash
# Requires docker buildx (for imagetools inspect)
scripts/update_base_image_digests.sh
```

The script resolves platform-agnostic OCI manifest-list digests for
`postgres:18.6-bookworm` and `postgres:18.7-bookworm` (so both `linux/amd64`
and `linux/arm64` builds pull the matching architecture automatically), patches
the Dockerfiles in-place, and prints the commit command. Include the
digest-update commit in the release PR.

---

Thanks again for taking the time to help improve `pg_trickle`.
