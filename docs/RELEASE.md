# Release Process

This document describes how to create a release of **pg_trickle**.

## Overview

Releases are automated via GitHub Actions. Pushing a version tag (`v*`)
triggers the [Release workflow](https://github.com/trickle-labs/pg-trickle/blob/main/.github/workflows/release.yml), which:

1. Runs version-sync and release qualification checks for the exact tag.
2. Builds extension packages for Linux (amd64 and arm64), macOS (arm64), and Windows (amd64).
3. Smoke-tests and qualifies the packaged Linux amd64 candidate against PostgreSQL 18.
4. Creates the GitHub Release and publishes the CNPG extension image only after qualification succeeds.

The release evidence binds each archive digest to the candidate commit. The
Linux amd64 archive receives the packaged live-runtime suites; Linux arm64,
macOS arm64, and Windows amd64 are reported as build-only unless their own
runtime suites are added and executed. v0.105.3 also reruns both DVM
regressions against the published v0.105.2 Linux package and requires those
negative controls to fail before it qualifies the new package.

The separate [GHCR image workflow](https://github.com/trickle-labs/pg-trickle/blob/main/.github/workflows/ghcr.yml)
and [PGXN workflow](https://github.com/trickle-labs/pg-trickle/blob/main/.github/workflows/pgxn.yml)
start only after the Release workflow succeeds for that tag. That workflow
publishes the main PostgreSQL image, including mutable `pg18` and `latest`
aliases, while PGXN publishes the source archive.

### v0.106.0 evidence and support tiers

The [v0.106.0 qualification contract](../tests/release/v0.106.0-qualification.json)
declares each required artifact and suite. The evidence writer accepts suite
records emitted by the release runner. It checks the candidate commit, package
digest, suite command, workload, test counts, PostgreSQL version, and retained
log digest before it can pass the release.

The contract gives Linux amd64 the runtime-qualified tier. Linux arm64 and
macOS arm64 are build-only. Windows amd64 is best-effort and optional. The
evidence manifest records build, install, runtime, upgrade, and reproducibility
status separately for every package. Reproducibility stays `not_checked` until
the workflow runs two clean builds and compares their outputs.

The live database suite runs keyed aggregation, skewed join aggregation, and a
small Graph V1 dependency graph. It compares source writes with no maintenance,
capture-only, and active refresh. Graph cases also run with output consumers
disabled and active. The suite fixes the table size, 25 ms write-batch cadence,
1 s active-refresh cadence, warmup, repetitions, and measurement window in the
contract. It checks exact results and differential strategy, then records write
latency, throughput, refresh latency, freshness, CPU, memory, WAL, backlog,
temporary spill, output-log growth, and database growth.

The workload gate limits p50 source-write latency overhead from active refresh
to the capture-only baseline to 15%. This isolates refresh work from the fixed
CDC capture cost. Criterion gates regressions above 10% when the absolute
increase is at least 50 ns; smaller percentage-only changes remain visible in
the retained evidence but do not fail the release. The release attaches raw
measurements, Criterion estimates, suite records, and logs. The manifest
distinguishes live runtime evidence from build-only status and static
upgrade-completeness checks.

## Prerequisites

- Push access to the repository (or a PR merged by a maintainer)
- All CI checks passing on `main` (verify the last run on the version-bump commit succeeded)
- The version in `Cargo.toml` matches the tag you intend to push
- Required GitHub secrets configured (see [Required GitHub Secrets](#required-github-secrets) below)

## Required GitHub Secrets

The release automation uses the following GitHub Actions secrets. Set them
under **Settings → Secrets and variables → Actions → New repository secret**.

| Secret | Used by | Description |
|--------|---------|-------------|
| `PGXN_USERNAME` | `pgxn.yml` | Your PGXN account username. Used to authenticate the `curl` upload to [PGXN Manager](https://manager.pgxn.org/) when publishing source archives to the [PostgreSQL Extension Network](https://pgxn.org/). Register at [pgxn.org](https://pgxn.org/). |
| `PGXN_PASSWORD` | `pgxn.yml` | Password for the PGXN account above. Never hardcode this — it must be stored as a secret so it is never exposed in logs or committed to the repository. |
| `CODECOV_TOKEN` | `coverage.yml` | Upload token for [Codecov](https://codecov.io/). Used to publish unit and E2E coverage reports. Obtain it from the Codecov dashboard after linking the repository. The workflow degrades gracefully (`fail_ci_if_error: false`) if absent. |
| `BENCHER_API_TOKEN` | `benchmarks.yml` | API token for [Bencher](https://bencher.dev/), the continuous benchmarking platform. Used to track Criterion benchmark results on `main` and detect regressions on pull requests. The benchmark steps are **skipped entirely** when this secret is absent, so CI still passes without it. Create a project at bencher.dev and copy the token from the project settings. |

> **Note:** The `GITHUB_TOKEN` secret is provided automatically by GitHub
> Actions and does not need to be configured manually. It is used by the
> release workflow to create GitHub Releases, by the Docker workflow to push
> images to GHCR, and by Bencher to post PR comments.

## Step-by-Step

### 1. Decide the version number

Follow [Semantic Versioning](https://semver.org/):

| Change type                        | Bump    | Example         |
|------------------------------------|---------|-----------------|
| Breaking SQL API or config change  | Major   | `1.0.0 → 2.0.0` |
| New feature, backward-compatible   | Minor   | `0.1.0 → 0.2.0` |
| Bug fix, no API change             | Patch   | `0.2.0 → 0.2.1` |
| Pre-release / release candidate    | Suffix  | `0.3.0-rc.1`     |

### 2. Update the version

Three files must have their version bumped together:

```bash
# 1. Cargo.toml — the canonical version source for the extension
#    Change:  version = "0.7.0"  →  version = "0.8.0"

# 2. META.json — the PGXN package metadata
#    Change both top-level "version" and the nested "provides" version

# 3. CHANGELOG.md
#    Rename ## [Unreleased] → ## [0.8.0] — YYYY-MM-DD
#    Add a new empty ## [Unreleased] section at the top
```

The `just check-version-sync` script enforces version consistency across the workspace.

The extension control file (`pg_trickle.control`) uses
`default_version = '@CARGO_VERSION@'`, which pgrx substitutes automatically at
build time — **no manual edit needed** there.

After editing, verify all version-related files are in sync:

```bash
just check-version-sync
```

### 3. Commit the version bump

```bash
git add Cargo.toml META.json CHANGELOG.md
git commit -m "release: v0.8.0"
git push origin main
```

### 4. Wait for CI to pass and verify upgrade completeness

Ensure the [CI workflow](https://github.com/trickle-labs/pg-trickle/blob/main/.github/workflows/ci.yml) passes on `main` with
the version bump commit. All unit, integration, E2E, and pgrx tests must be
green.

**Critical:** Before tagging, verify that the upgrade script covers all SQL schema changes:

```bash
# Run comprehensive upgrade completeness checks
just check-upgrade-all

# If any check fails (e.g. "ERROR: X new function(s) missing from upgrade script"),
# fix the issue by adding the missing SQL objects to:
#   sql/pg_trickle--<prev>--<new>.sql
#
# Then re-run until all checks pass:
just check-upgrade-all  # Should print "All 15 upgrade step(s) passed completeness checks."
```

**Why this matters:** New SQL functions, views, tables, and columns added in any prior
release must be carried forward in the upgrade script, even if the current release
doesn't change them. The upgrade script is the source of truth for what PostgreSQL
applies when users run `ALTER EXTENSION pg_trickle UPDATE`.

Confirm the local and CI upgrade-E2E defaults were advanced to the new release:

```bash
just check-version-sync  # Verifies ci.yml, justfile, and test defaults
```

### 5. Create and push the tag

```bash
git tag -a v0.2.0 -m "Release v0.2.0"
git push origin v0.2.0
```

This triggers the Release workflow automatically.

### 6. Monitor the release

Watch the [Actions tab](https://github.com/trickle-labs/pg-trickle/actions/workflows/release.yml) for progress.
The release workflow runs these jobs in order:

```
preflight  ──►  build-release ──► test-release ──┐
                      └────────► qualification ──┼─► publish-release
                                                └─► publish-docker-arch ─► publish-docker
                                                       \
                                  publication-result ◄──┘

successful Release completion ──► GHCR aliases + PGXN source archive
```

The release workflow's `publish-release` and `publish-docker-arch` jobs both
require successful qualification. `publication-result` fails the workflow if
any required stage failed, was cancelled, or was skipped. Its successful
completion triggers the GHCR and PGXN workflows, which check out the exact tag
commit. A failed, skipped, or cancelled qualification therefore cannot
promote any official release channel.

### 7. Make the GHCR package public (first release only)

When a package is pushed to GHCR for the first time it is **private** by
default. Because this is an open-source project, packages linked to the
public repository inherit public visibility — but you must make the package
public once to unlock that:

1. Go to **github.com/⟨owner⟩ → Packages → pg_trickle-ext**
2. Click **Package settings**
3. Scroll to **Danger Zone** → **Change package visibility** → set to **Public**

After that first change:
- All future pushes keep the package public automatically
- Unauthenticated `docker pull ghcr.io/trickle-labs/pg_trickle-ext:...` works
- Storage and bandwidth are free (GHCR open-source advantage)
- The package page shows the README, linked repository, license, and
  description from the OCI labels

### 8. Verify the release

Once both workflows complete:

- [ ] Check the [GitHub Releases](https://github.com/trickle-labs/pg-trickle/releases) page for the new release
- [ ] Verify all four platform archives are attached: Linux amd64/arm64 and macOS arm64 (`.tar.gz`), plus Windows amd64 (`.zip`)
- [ ] Verify `SHA256SUMS.txt` is present
- [ ] Verify `RELEASE-EVIDENCE.json` reports the test tier for each artifact
- [ ] Verify the extension image is available at `ghcr.io/trickle-labs/pg_trickle-ext:<version>`
- [ ] Verify the PGXN upload succeeded: `pgxn info pg_trickle` should show the new version
- [ ] Optionally verify the extension image layout:

```bash
docker pull ghcr.io/trickle-labs/pg_trickle-ext:<version>
ID=$(docker create ghcr.io/trickle-labs/pg_trickle-ext:<version>)
docker cp "$ID:/lib/" /tmp/ext-lib/
docker cp "$ID:/share/" /tmp/ext-share/
docker rm "$ID"
ls -la /tmp/ext-lib/ /tmp/ext-share/extension/
```

---

## Post-Release Checklist

Complete these steps immediately after a release tag has been pushed and both the Release and PGXN workflows have finished successfully.

- [ ] **Create a post-release branch** from `main` (e.g. `post-release-<ver>-a`)
- [ ] **Bump `Cargo.toml`** `version` to the next development version (e.g. `0.12.0` → `0.13.0`)
- [ ] **Bump `META.json`** — both the top-level `"version"` and the nested `"provides" → "pg_trickle" → "version"` to match
- [ ] **Write `plans/PLAN_0_<next>_0.md`** — initial planning document for the next milestone
- [ ] **Delete `plans/PLAN_0_<released>_0.md`** — remove the now-completed plan
- [ ] **Wrap roadmap items** — in `ROADMAP.md`, wrap all completed items from the old release with `<details>` tags to archive them
- [ ] **Add `## [Unreleased]` stub** to `CHANGELOG.md` above the just-released entry
- [ ] **Create `sql/pg_trickle--<released>--<next>.sql`** — empty upgrade script stub for the next migration hop
- [ ] **Copy `sql/archive/pg_trickle--<released>.sql` → `sql/archive/pg_trickle--<next>.sql`** — placeholder archive baseline for the next version
- [ ] **Update `justfile`** — advance `build-upgrade-image` and `test-upgrade` `to` defaults to `<next>`; update the `build-hub` Docker image tag
- [ ] **Update `tests/e2e_upgrade_tests.rs`** — advance all `unwrap_or("<released>".into())` fallback strings to `<next>`
- [ ] **Update version numbers in `README.md`** — search for occurrences of the released version (e.g. `0.17.0`) and advance them to `<next>`: CNPG image reference (`ghcr.io/trickle-labs/pg_trickle-ext:<version>`), dbt `revision` tag, and any other hardcoded version strings. A quick check: `grep -n '<released>' README.md`
- [ ] **Run `just check-version-sync`** — must exit 0 before opening the PR
- [ ] **Open a PR** against `main` with the commit title `chore: start v<next> development cycle`

---

## Preparing for the Next Release (Pre-Work Checklist)

Use this checklist at the start of each new release milestone to ensure the repository is properly set up before development begins. This maps directly to what `just check-version-sync` verifies.

| File / target | Action | `check-version-sync` check |
|---------------|--------|---------------------------|
| `Cargo.toml` | `version = "<next>"` | canonical version source |
| `META.json` | both `"version"` fields set to `<next>` | PGXN manifest |
| `CHANGELOG.md` | `## [Unreleased]` section present | (manual hygiene) |
| `sql/pg_trickle--<prev>--<next>.sql` | stub file exists | upgrade SQL exists |
| `sql/archive/pg_trickle--<next>.sql` | placeholder file exists (copy of `<prev>`) | archive SQL exists |
| `.github/workflows/ci.yml` | upgrade matrix and chain end at `<next>` | CI matrix up to date |
| `justfile` | `build-upgrade-image` and `test-upgrade` `to` defaults = `<next>` | justfile defaults |
| `tests/e2e_upgrade_tests.rs` | all `unwrap_or` fallbacks = `"<next>"` | e2e fallback strings |

Quick-verify with:

```bash
just check-version-sync
# Should print: All version references are in sync.
```

---

## Release Artifacts

Each release produces:

| Artifact | Description |
|----------|-------------|
| `pg_trickle-<ver>-pg18-linux-amd64.tar.gz` | Extension files for Linux x86_64 |
| `pg_trickle-<ver>-pg18-macos-arm64.tar.gz` | Extension files for macOS Apple Silicon |
| `pg_trickle-<ver>-pg18-windows-amd64.zip`  | Extension files for Windows x64 |
| `SHA256SUMS.txt` | SHA-256 checksums for all archives |
| `ghcr.io/trickle-labs/pg_trickle-ext:<ver>` | CNPG extension image for Image Volumes (amd64 + arm64) |

### Installing from an archive

```bash
tar xzf pg_trickle-<version>-pg18-linux-amd64.tar.gz
cd pg_trickle-<version>-pg18-linux-amd64

sudo cp lib/*.so "$(pg_config --pkglibdir)/"
sudo cp extension/*.control extension/*.sql "$(pg_config --sharedir)/extension/"
```

Then add to `postgresql.conf` and restart:

```
shared_preload_libraries = 'pg_trickle'
```

See [Installation](installation.md) for full installation details.

## Pre-releases

Tags containing `-rc`, `-beta`, or `-alpha` (e.g., `v0.3.0-rc.1`) are
automatically marked as pre-releases on GitHub. Pre-release extension images are
tagged but do **not** update the `latest` tag.

## Hotfix Releases

For urgent fixes on an older release:

```bash
# Branch from the tag
git checkout -b hotfix/v0.2.1 v0.2.0

# Apply fix, bump version to 0.2.1
git commit -am "fix: ..."
git push origin hotfix/v0.2.1

# Tag from the branch (CI will still run the release workflow)
git tag -a v0.2.1 -m "Release v0.2.1"
git push origin v0.2.1
```

## Files to Update for Each Release

Every release requires manual updates to the files below. Missing any of them leads to version skew between the code, the docs, and the packages.

| File | What to change | Why |
|------|----------------|-----|
| `Cargo.toml` | `version = "x.y.z"` field | The canonical version source. pgrx reads this at build time and substitutes it into `pg_trickle.control` via `@CARGO_VERSION@`. The git tag must match. |
| `META.json` | Both `"version"` fields (top-level and inside `"provides"`) | The PGXN package manifest. The `pgxn.yml` workflow uploads this file as part of the source archive; a stale version here means the wrong version appears on pgxn.org. |
| `CHANGELOG.md` | Rename `## [Unreleased]` → `## [x.y.z] — YYYY-MM-DD`; add a new empty `## [Unreleased]` at the top | Keeps the public changelog accurate and gives downstream users a dated record of changes. |
| `ROADMAP.md` | Update the preamble's latest-release/current-milestone lines; mark the released milestone done; advance the "We are here" pointer to the next milestone | Keeps the forward-looking plan aligned with reality. Leaves no confusion about what just shipped versus what is next. |
| `README.md` | Update test-count line (`~N unit tests + M E2E tests`) if test counts changed significantly | The README is the first thing users read; stale numbers erode trust. |
| `INSTALL.md` | Update any version numbers in install commands or example URLs | Users copy-paste installation commands; stale versions cause failures. |
| `docs/UPGRADING.md` | Add the new version-specific migration notes and extend the supported upgrade-path table | Documents exactly what `ALTER EXTENSION ... UPDATE` will do and which chains are supported. |
| `sql/pg_trickle--<old>--<new>.sql` | Add or update the hand-authored upgrade script for every SQL-surface change (new objects, changed signatures, changed defaults, view changes). **Also carry forward all functions/views/tables added in previous releases — the upgrade script is cumulative.** | `ALTER EXTENSION ... UPDATE` only applies what is explicitly scripted; function defaults and signatures stored in `pg_proc` do not update themselves. Omitting a function that existed in `<old>` but is expected in `<new>` will break user upgrades. |
| `sql/archive/pg_trickle--<new>.sql` | Regenerate and commit the full-install SQL baseline for the new version. **This file was created as a placeholder copy of `<prev>` at the start of the development cycle — it must be replaced with the actual generated SQL before tagging.** Run `cargo pgrx schema` (or the equivalent `just` target) to produce the final schema, then overwrite the placeholder. | Future upgrade-completeness checks and upgrade E2E tests need an exact baseline for the released version. A stale placeholder from the start of the cycle will cause spurious failures. |
| `.github/workflows/ci.yml`, `justfile`, `tests/build_e2e_upgrade_image.sh`, `tests/Dockerfile.e2e-upgrade` | Advance the upgrade-check chain and default upgrade-E2E target version to the new release | Prevents release automation and local upgrade validation from getting stuck on the previous version after a new migration hop is added. |
| `pg_trickle.control` | **No manual edit needed** — `default_version` is set to `'@CARGO_VERSION@'` and pgrx substitutes it at build time. Verify the substitution in the built artifact. | Ensures the SQL `CREATE EXTENSION` command installs the right version. |

> **CRITICAL:** After updating `sql/pg_trickle--<old>--<new>.sql`, always run
> `just check-upgrade-all` to verify that the upgrade script is complete. This
> checks not just the immediate hop to the new version, but the entire upgrade
> chain from v0.1.3 onwards. If the check fails (e.g. "ERROR: 3 new function(s)
> missing"), it means the upgrade script is missing one or more SQL objects that
> users will expect to have after upgrading. Fix all failures before tagging.

### Checklist summary

```
[ ] Cargo.toml — version bumped
[ ] META.json — both "version" fields updated to match
[ ] CHANGELOG.md — [Unreleased] renamed to [x.y.z] with date; new empty [Unreleased] added
[ ] ROADMAP.md — preamble updated; released milestone marked done
[ ] README.md — test counts current (if materially changed)
[ ] INSTALL.md — version references current
[ ] docs/UPGRADING.md — latest migration notes and supported chains added
[ ] sql/pg_trickle--<old>--<new>.sql — covers every SQL-surface change AND carries forward all previous release functions
[ ] sql/archive/pg_trickle--<new>.sql — regenerated from final schema and committed (replaces the dev-cycle placeholder)
[ ] just check-upgrade-all — all upgrade steps pass completeness checks (not just the one-step hop)
[ ] Upgrade automation defaults — CI/local upgrade checks and E2E target the new version
[ ] just check-version-sync — all version references in sync
[ ] All CI checks on main have passed (verify the last run on the version-bump commit succeeded)
[ ] git tag matches Cargo.toml version
```

---

## Troubleshooting

### Release workflow failed

Go to the [Actions tab](https://github.com/trickle-labs/pg-trickle/actions/workflows/release.yml) and identify
which job failed. Then follow the appropriate recovery path below.

#### Option A: Re-run (transient failure)

If the failure is transient — network timeout, registry hiccup, runner
issue — you can re-run without changing anything:

1. Open the failed workflow run in the **Actions** tab
2. Click **Re-run all jobs** (or re-run just the failed job)

This works because the `v*` tag still points to the same commit, and the
workflow uses `cancel-in-progress: false` so a re-run won't be cancelled.

#### Option B: Fix code and re-tag

If the failure is a real build or code issue:

```bash
# 1. Delete the remote tag
git push origin :refs/tags/v0.2.0

# 2. Delete the local tag
git tag -d v0.2.0

# 3. Fix the issue, commit, and push
git add <files>
git commit -m "fix: ..."
git push origin main

# 4. Re-tag on the new commit and push
git tag -a v0.2.0 -m "Release v0.2.0"
git push origin v0.2.0
```

This triggers a fresh release workflow run.

#### Option C: Clean up a partial GitHub Release

If the workflow created a draft or partial Release before failing:

1. Go to **Releases** in the repository
2. Delete the broken release (this does **not** delete the tag)
3. Then follow Option A or Option B above

### Upgrade script completeness check failed

If `just check-upgrade-all` reports errors like `"ERROR: X new function(s) missing
from upgrade script"`, it means the upgrade SQL script is incomplete:

```bash
# 1. Look at the error — it tells you exactly what's missing
just check-upgrade-all  # e.g. "ERROR: 3 new function(s) missing from upgrade script:
                        #        - pgtrickle.\"explain_refresh_mode\"
                        #        - pgtrickle.\"fuse_status\"
                        #        - pgtrickle.\"reset_fuse\""

# 2. Find where those objects are defined in the previous release
#    (they should already exist in sql/archive/pg_trickle--<prev>.sql)
grep -n "CREATE.*FUNCTION.*explain_refresh_mode" sql/archive/pg_trickle--*.sql

# 3. Copy the function definitions (CREATE OR REPLACE FUNCTION) to the
#    upgrade script you're fixing. They should go into:
#    sql/pg_trickle--<old>--<new>.sql
#    
#    Typically, carry-forward functions are grouped in their own section
#    at the top of the upgrade script with a comment explaining they're
#    from a prior release.

# 4. Re-run the check to verify it passes
just check-upgrade-all
```

**Why this happens:** When a new release (e.g. v0.11.0) adds SQL functions, those
functions must be explicitly included in all subsequent upgrade scripts. The upgrade
script is the ground truth — PostgreSQL only applies what is listed in the `.sql` file.
If you skip a function that users expect, their upgraded extension will be missing
that object.

#### Common failure causes

| Symptom | Cause | Fix |
|---------|-------|-----|
| Version mismatch error | `Cargo.toml` version doesn't match the git tag | Run `just check-version-sync`, fix any skew, commit, delete tag, re-tag (Option B) |
| Build failure | Compilation error in release profile | Fix on `main`, re-tag (Option B) |
| Docker push failed | Missing permissions | Verify `packages: write` is in the workflow and `GITHUB_TOKEN` has GHCR access, then re-run (Option A) |
| Smoke test failed | Extension doesn't load in PostgreSQL | Fix the issue, re-tag (Option B) |
| PGXN upload failed | Missing `PGXN_USERNAME` / `PGXN_PASSWORD` secrets, or `META.json` version not updated | Add the secrets in repository settings; verify `META.json` version matches the tag; re-run the `pgxn.yml` workflow from the Actions tab |
| `just check-upgrade-all` reports missing functions/views | Upgrade script is incomplete — new objects from prior releases not carried forward | See "Upgrade script completeness check failed" above for recovery steps |
| Rate limited | GitHub API or GHCR throttling | Wait a few minutes, then re-run (Option A) |

### Yanking a release

If a release has a critical issue:

1. Mark it as pre-release on the GitHub Releases page (uncheck "Set as the latest release")
2. Add a warning to the release notes
3. Publish a patch release with the fix
