# PLAN_AGENT_SKILLS.md — Agent Skill Proposals for pg_trickle

**Status:** � In progress
**Last updated:** 2026-04-15

---

## Motivation

The pg_trickle project has a rich, multi-layered development workflow spanning
Rust extension code, SQL migration scripts, dbt Jinja macros,
Docker image building, benchmarking, and release management. Currently three
agent skills exist:

1. **create-pull-request** — Safe PR creation with Unicode-correct body files
2. **enrich-release-roadmap** — Roadmap enrichment across six quality pillars
3. **implement-roadmap-version** — ✅ Created 2026-04-15 — Full implementation loop for a release milestone
4. **improve-documentation** — ✅ Created 2026-05-22 — Thorough docs review: accuracy, links, gaps, consistency, duplication

Many recurring development tasks are complex enough to benefit from codified
agent skills — structured procedures that encode project-specific conventions
and guard rails. This plan proposes 12 new skills, prioritised by frequency of
use and error-proneness.

> **`implement-roadmap-version`** has been implemented. See
> `.github/skills/implement-roadmap-version/SKILL.md`.

---

## Proposed Skills

### Skill 1 — `write-upgrade-migration`

**Priority:** P0 (must-have)
**Effort:** M
**Triggers:** Adding/removing SQL functions, altering catalog tables, changing
function signatures, bumping the extension version.

**Description:**
Automates creation of `sql/pg_trickle--<from>--<to>.sql` upgrade migration
files. The skill should:

1. Read the current version from `pg_trickle.control` and `Cargo.toml`.
2. Diff the `#[pg_extern]` signatures between `main` and the working branch
   to identify added, removed, and changed functions.
3. Diff `CREATE TABLE` / `ALTER TABLE` statements for catalog tables in
   `pgtrickle` schema.
4. Generate the migration SQL with proper `CREATE OR REPLACE FUNCTION`,
   `ALTER TABLE`, and `DROP FUNCTION` statements.
5. Validate the migration with `just check-upgrade-all`.
6. Warn if the version in `pg_trickle.control` hasn't been bumped.

**Guard rails:**
- Never generate `DROP TABLE` without explicit user confirmation.
- Always include `IF EXISTS` / `IF NOT EXISTS` guards.
- Run `just check-upgrade-all` as a post-step validation.

**Files involved:**
- `sql/pg_trickle--*--*.sql` (output)
- `pg_trickle.control`, `Cargo.toml` (version source)
- `src/api/*.rs`, `src/catalog.rs` (function/table definitions)
- `scripts/check_upgrade_completeness.sh` (validation)

---

### Skill 2 — `add-sql-function`

**Priority:** P0 (must-have)
**Effort:** M
**Triggers:** Implementing a new SQL-callable function, exposing Rust logic to
the SQL API.

**Description:**
End-to-end workflow for adding a new `#[pg_extern]` function with all the
project conventions enforced:

1. Scaffold the function in the appropriate `src/api/*.rs` module with
   `#[pg_extern(schema = "pgtrickle")]`.
2. Add the function to the upgrade migration SQL
   (`sql/pg_trickle--<prev>--<current>.sql`).
3. Add the function to `sql/pg_trickle--<current>.sql` (full install script).
4. Create a stub E2E test in `tests/e2e_*_tests.rs` following the
   `test_<component>_<scenario>_<expected>` naming.
5. Update `docs/SQL_REFERENCE.md` with the function signature and description.
6. Run `just lint` and `just test-unit` as validation.

**Guard rails:**
- Enforce `Result<T, PgTrickleError>` return type — no `unwrap()`.
- SPI blocks must follow short-lived connection pattern.
- Cast `name`-typed columns to `text` in any SPI queries.

**Files involved:**
- `src/api/mod.rs` or `src/api/helpers.rs`
- `sql/pg_trickle--*.sql`
- `tests/e2e_*_tests.rs`
- `docs/SQL_REFERENCE.md`

---

### Skill 3 — `run-test-tier`

**Priority:** P0 (must-have)
**Effort:** S
**Triggers:** "Run tests", "check if this works", verifying a change, CI
failure triage.

**Description:**
Intelligent test runner that selects the appropriate test tier based on what
files changed, then runs the minimal set of tests:

1. Inspect `git diff --name-only main` to classify changed files.
2. Map changes to test tiers:
   - `src/**` (non-test) → `just test-unit` + `just test-light-e2e`
   - `src/dvm/**` → also `just test-dvm`
   - `tests/e2e_*` → `just test-light-e2e` or `just test-e2e`
   - `sql/**` → `just check-upgrade-all` + `just test-light-e2e`
   - `dbt-pgtrickle/**` → `just test-dbt-fast`
   - `pgtrickle-tui/**` → *(removed — TUI deleted)*
   - `benches/**` → `just bench`
3. Run the selected tiers in dependency order.
4. Parse output and surface failures with file/line context.
5. Suggest `just test-e2e` or `just test-tpch` for deeper coverage when
   changes touch refresh/DVM core.

**Guard rails:**
- Never run `just test-all` by default (too slow) — escalate only if asked.
- Warn if Docker is required but not running.
- Suggest `just build-e2e-image` when the E2E image is stale.

---

### Skill 4 — `write-e2e-test`

**Priority:** P1 (high)
**Effort:** M
**Triggers:** Adding test coverage for a stream table scenario, verifying a
bug fix, covering a new SQL feature.

**Description:**
Scaffolds a well-structured E2E test following project conventions:

1. Ask for the scenario (table DDL, view definition, DML operations, expected
   outcome).
2. Determine the target test file (existing `tests/e2e_*_tests.rs` or new).
3. Generate the test using:
   - `#[tokio::test]` attribute
   - `test_<component>_<scenario>_<expected>` naming
   - `tests/common/mod.rs` helpers for container setup
   - Proper `CREATE TABLE`, `pgtrickle.create_stream_table()`, DML, refresh,
     and assertion pattern.
4. Classify as light E2E (stock postgres) or full E2E (custom image) based on
   features used (e.g. parallel refresh requires full image).
5. Run the test with the appropriate tier to verify it passes.

**Guard rails:**
- Tests must be deterministic — no `random()`, no timing-dependent assertions.
- Use `assert_eq!` with clear messages including actual vs expected values.
- Clean up stream tables in test teardown.

**Files involved:**
- `tests/e2e_*_tests.rs`
- `tests/common/mod.rs` (shared helpers)

---

### Skill 5 — `diagnose-ci-failure`

**Priority:** P1 (high)
**Effort:** M
**Triggers:** "CI is red", "this test failed", investigating a GitHub Actions
failure.

**Description:**
Structured workflow for diagnosing CI failures:

1. Fetch the failed workflow run: `gh run view <id> --log-failed`.
2. Identify the failed job and step.
3. Classify the failure type:
   - **Compile error** → locate the file and line, suggest fix.
   - **Clippy warning** → find the lint code and remediation.
   - **Test failure** → extract the assertion, diff expected vs actual.
   - **Docker build failure** → check Dockerfile layer cache, apt/cargo errors.
   - **Timeout** → check for deadlocks, long-running SPI, missing `SIGTERM`.
   - **Flaky test** → check `gh run list` for recent pass/fail pattern.
4. Cross-reference with known issues (`TROUBLESHOOTING.md`, `ERRORS.md`).
5. Suggest a fix with file paths and code changes.
6. Recommend which test tier to re-run locally for verification.

**Guard rails:**
- Always check the full error context (5+ lines before/after).
- For flaky tests, check at least 3 recent runs before declaring flaky.
- Never suggest disabling a test as a fix.

---

### Skill 6 — `add-dbt-macro`

**Priority:** P1 (high)
**Effort:** M
**Triggers:** Adding a new dbt macro, wrapping a new pg_trickle SQL function
in dbt, extending the dbt package.

**Description:**
Scaffolds a new dbt macro following the `dbt-pgtrickle/AGENTS.md` conventions:

1. Create the macro file with `pgtrickle_` prefix naming.
2. Use `{% call statement(..., auto_begin=False, fetch_result=False) %}` for
   DDL operations (never `run_query()`).
3. Guard `run_query()` with `{% if execute %}` for read-only queries.
4. Use `dbt.string_literal()` for all string parameters.
5. Add integration test coverage in `dbt-pgtrickle/integration_tests/`.
6. Update the dbt package documentation.
7. Run `just test-dbt-fast` to verify.

**Guard rails:**
- DDL must use explicit `BEGIN; ... COMMIT;` in call statements.
- Never use `run_query()` for `CREATE`, `ALTER`, `DROP`, or `SELECT` that
  calls pg_trickle mutating functions.
- Validate Jinja whitespace control (`{%- -%}` for control, `{{ }}` for output).

**Files involved:**
- `dbt-pgtrickle/macros/**/*.sql`
- `dbt-pgtrickle/integration_tests/models/**`
- `dbt-pgtrickle/integration_tests/tests/**`

---

### Skill 7 — `benchmark-change`

**Priority:** P1 (high)
**Effort:** M
**Triggers:** Performance-sensitive change to DVM, refresh, CDC, or scheduler;
verifying no regression; comparing two approaches.

**Description:**
Runs the appropriate benchmark suite and interprets results:

1. Identify the affected subsystem from changed files.
2. Select the benchmark tier:
   - `src/dvm/**` → `just bench-diff` (Criterion micro-benchmarks)
   - `src/refresh.rs`, `src/scheduler.rs` → `just bench`
   - `src/dag.rs` → `just test-dag-bench`
   - Broad changes → `just test-bench-e2e-fast`
   - TPC-H queries → `just bench-tpch-fast`
3. Save a baseline on `main`: `git stash && just bench && git stash pop`.
4. Run the benchmark on the working branch.
5. Compare results using `scripts/bench_compare.sh` or
   `scripts/criterion_regression_check.py`.
6. Summarize: which operations improved, regressed, or stayed flat, with
   percentage changes and statistical significance.

**Guard rails:**
- Warn if the machine is under load (other Docker containers, compilations).
- Flag any regression > 5% as needing investigation.
- Suggest TPC-H validation for changes to the DVM operator pipeline.

---

### Skill 8 — `write-plan`

**Priority:** P1 (high)
**Effort:** S
**Triggers:** Starting a new feature, spike, or investigation; creating a plan
document for a body of work.

**Description:**
Scaffolds a plan document in `plans/` following the established format:

1. Ask for the scope (feature, refactor, investigation, performance, testing).
2. Choose the right subdirectory (`plans/`, `plans/performance/`,
   `plans/testing/`, `plans/sql/`, etc.).
3. Generate the document with standard sections:
   - Title, status badge, last updated
   - Milestone goals (high-level outcomes)
   - Recommended implementation order (phased table)
   - Implementation status checklist
   - Release/exit criteria
4. Cross-reference related plans and ADRs.
5. Add the plan to `plans/INDEX.md`.

**Files involved:**
- `plans/*.md` or `plans/<subdir>/*.md` (output)
- `plans/INDEX.md` (index update)
- `plans/README.md` (navigation)

---

### Skill 9 — `add-error-variant`

**Priority:** P2 (nice-to-have)
**Effort:** S
**Triggers:** Handling a new error condition, improving error messages,
adding context to user-facing errors.

**Description:**
Adds a new `PgTrickleError` variant with full wiring:

1. Add the variant to `src/error.rs` with descriptive fields.
2. Implement `Display` for the variant with actionable error message
   (include table name, query fragment, remediation hint).
3. Add SQLSTATE mapping if appropriate.
4. Wire up `From` conversions if wrapping an upstream error.
5. Add the error to `docs/ERRORS.md` with code, meaning, and resolution.
6. Add a unit test verifying the error message format.

**Guard rails:**
- Error messages must include context (table name, column, OID where relevant).
- Never use generic "internal error" — always provide actionable detail.
- Run `just test-unit` to verify compilation and test.

**Files involved:**
- `src/error.rs`
- `docs/ERRORS.md`
- Calling module that raises the error

---

### Skill 10 — `review-unsafe`

**Priority:** P2 (nice-to-have)
**Effort:** S
**Triggers:** Adding or modifying `unsafe` code, auditing safety, pre-release
review.

**Description:**
Audits `unsafe` blocks in the codebase:

1. Run `scripts/unsafe_inventory.sh` to get current counts.
2. For each `unsafe` block in the changed files:
   - Verify a `// SAFETY:` comment exists and is accurate.
   - Check that the invariants claimed in the comment actually hold.
   - Verify the block is minimal (no safe operations inside `unsafe`).
   - Check if a safe abstraction wrapper could eliminate the `unsafe`.
3. Compare counts against baseline to detect drift.
4. Suggest safe alternatives where possible (pgrx safe wrappers, etc.).

**Guard rails:**
- Every `unsafe` block MUST have a `// SAFETY:` comment.
- Flag any `unsafe` that can be replaced with a safe pgrx API.
- Fail if the unsafe count exceeds the baseline without justification.

**Files involved:**
- `src/**/*.rs` (all source files)
- `scripts/unsafe_inventory.sh`

---

### Skill 11 — `prepare-release`

**Priority:** P2 (nice-to-have)
**Effort:** L
**Triggers:** Cutting a new release, pre-release checklist, version bump.

**Description:**
End-to-end release preparation checklist:

1. **Version bump:** Update version in `Cargo.toml`, `pg_trickle.control`,
   `META.json`, and dbt `dbt_project.yml`.
2. **Changelog:** Generate changelog from `git log` since last tag, categorize
   by type (features, fixes, performance, breaking changes).
3. **Upgrade migration:** Verify `sql/pg_trickle--<prev>--<new>.sql` exists
   and is complete (`just check-upgrade-all`).
4. **Version sync:** Run `just check-version-sync` to verify consistency.
5. **Test gate:** Run `just test-all` and confirm green.
6. **Documentation:** Update `docs/UPGRADING.md` with migration notes, update
   `ROADMAP.md` to mark items as shipped.
7. **Pre-deployment:** Walk through `docs/PRE_DEPLOYMENT.md` checklist.
8. **Docker images:** Verify `Dockerfile.hub` and `Dockerfile.ghcr` build.
9. **Tag:** Suggest `git tag -a v<version> -m "..."`.

**Guard rails:**
- Block if any test tier fails.
- Block if upgrade migration is missing or incomplete.
- Block if version strings are inconsistent across files.
- Warn if `CHANGELOG.md` has uncommitted entries.

**Files involved:**
- `Cargo.toml`, `pg_trickle.control`, `META.json`
- `dbt-pgtrickle/dbt_project.yml`
- `CHANGELOG.md`, `ROADMAP.md`
- `docs/UPGRADING.md`, `docs/PRE_DEPLOYMENT.md`
- `sql/pg_trickle--*.sql`

---

### Skill 12 — `add-guc`

**Priority:** P2 (nice-to-have)
**Effort:** S
**Triggers:** Adding a new configuration parameter, exposing a tunable to
users.

**Description:**
Adds a new GUC (Grand Unified Configuration) variable:

1. Define the GUC in `src/config.rs` with appropriate type, default, min/max,
   and description.
2. Register it in `_PG_init()` in `src/lib.rs`.
3. Add it to `docs/CONFIGURATION.md` with description, type, default, range,
   and when to change it.
4. If the GUC affects background workers, ensure it's checked at the right
   point (startup vs. per-cycle).
5. Add a test verifying the default value and boundary behavior.
6. Add the GUC to upgrade migration SQL if it requires `ALTER SYSTEM` support.

**Guard rails:**
- GUCs must have sensible defaults that work for the common case.
- Include `context` (PGC_SUSET, PGC_USERSET, etc.) appropriate to the setting.
- Document in both inline comment and `CONFIGURATION.md`.

**Files involved:**
- `src/config.rs`
- `src/lib.rs`
- `docs/CONFIGURATION.md`

---

## Priority Summary

| Priority | Skills | Count |
|----------|--------|-------|
| P0 | write-upgrade-migration, add-sql-function, run-test-tier | 3 |
| P1 | write-e2e-test, diagnose-ci-failure, add-dbt-macro, benchmark-change, write-plan | 5 |
| P2 | add-error-variant, review-unsafe, prepare-release, add-guc | 4 |

---

## Recommended Implementation Order

Front-load the skills that reduce the most errors and are used most frequently.

| Order | Skill | Effort | Rationale |
|-------|-------|--------|-----------|
| 1 | run-test-tier | S | Used on every change; reduces wasted CI cycles |
| 2 | write-upgrade-migration | M | Most error-prone manual task; blocks every release |
| 3 | add-sql-function | M | Touches 5+ files; easy to miss a step |
| 4 | write-e2e-test | M | Standardises test patterns; used frequently |
| 5 | diagnose-ci-failure | M | High-value for unblocking development |
| 6 | write-plan | S | Quick to build; standardises plan documents |
| 7 | add-dbt-macro | M | Critical for dbt package development |
| 8 | benchmark-change | M | Needed for performance-sensitive work |
| 9 | add-error-variant | S | Small but frequently needed |
| 10 | add-guc | S | Small; well-scoped |
| 11 | review-unsafe | S | Pre-release safety; periodic use |
| 12 | prepare-release | L | Complex but infrequent; high value per use |

---

## Skill File Structure

Each skill should be created at `.github/skills/<name>/SKILL.md` following the
existing format:

```
.github/skills/<name>/SKILL.md
```

YAML frontmatter:
```yaml
---
name: <skill-name>
description: "<one-line description>"
argument-hint: "<example invocation context>"
---
```

Followed by:
- **Title** and purpose
- **When to Use** section with trigger conditions
- **Prerequisites** (tools, environment, state)
- **Procedure** with numbered steps
- **Guard Rails** (what must never happen)
- **Checklist** for validation

---

## Exit Criteria

- [ ] All 12 skill SKILL.md files created under `.github/skills/`
- [ ] Each skill tested with at least one real invocation
- [ ] `AGENTS.md` updated to reference the new skills in the relevant sections
- [ ] Skills listed in the copilot-instructions or `.github/copilot-instructions.md`

---

## Dependencies

- No external dependencies.
- Skills 1–3 can be built in parallel.
- Skill 6 (add-dbt-macro) depends on familiarity with `dbt-pgtrickle/AGENTS.md`.
- Skill 11 (prepare-release) builds on skills 1, 3, and 7.
