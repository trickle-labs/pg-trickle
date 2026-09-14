# Migration Report: `grove/pg-trickle` → `trickle-labs/pg-trickle`

**Date:** 2026-05-06  
**Scope:** Full audit of every hardcoded reference that must change when the
GitHub repository is transferred from the `grove` organisation to
`trickle-labs`.

---

## 1. Executive Summary

Moving to `trickle-labs/pg-trickle` requires changes in **five categories**:

| Category | Files affected | Notes |
|---|---|---|
| Rust source + Cargo manifests | 4 | Issue URLs in error messages; `Cargo.toml` metadata |
| Dockerfiles + CI workflows | 8 | Builder image, GHCR image path, vendor label |
| Metadata + book config | 3 | `META.json`, `book.toml`, `ESSENCE.md` |
| Documentation (mdBook + docs/) | ~30 | All `github.com/grove/pg-trickle` and `grove.github.io/pg-trickle` links |
| Blog posts (100 + files) | ~100 | Every post header + some footers |
| dbt adapter files | 4 | `README.md`, `AGENTS.md`, `packages.yml` examples |
| Plan / roadmap / historical docs | ~15 | Issue links, image refs, comparison tables |
| Docker Compose / playground | 3 | `ghcr.io/grove/pg_trickle:latest` image refs |

In addition there are **seven external-service actions** that must be taken
independently of code changes (see §5).

---

## 2. File-by-File Change Inventory

### 2.1 Rust source and Cargo manifests

| File | Change needed |
|---|---|
| `Cargo.toml` (lines 7–8) | `repository` and `homepage` → `https://github.com/trickle-labs/pg-trickle` |
| `pgtrickle-relay/Cargo.toml` (line 7) | Same `repository` field |
| `src/api/mod.rs` (lines 397, 557) | Bug-report URL in user-facing error strings: `github.com/grove/pg-trickle/issues` → `github.com/trickle-labs/pg-trickle/issues` |
| `src/cdc.rs` (line 716) | Code comment issue link: same replacement |

> **Impact on compiled output:** The `api.rs` changes affect the text emitted
> by `ereport!()` calls, which users see in PostgreSQL log output.  They are
> therefore worth updating promptly so users who copy-paste issue links get the
> right destination.

---

### 2.2 Dockerfiles

| File | Line(s) | Change needed |
|---|---|---|
| `Dockerfile.ghcr` | 5, 17, 22, 26 | Comments referencing `ghcr.io/grove/pg_trickle` |
| `Dockerfile.ghcr` | 111 | `ARG REPO_URL=https://github.com/grove/pg-trickle` |
| `cnpg/Dockerfile.ext` | 19 | `ARG REPO_URL=https://github.com/grove/pg-trickle` |

---

### 2.3 GitHub Actions workflows

| File | Line(s) | Change needed |
|---|---|---|
| `.github/workflows/builder-image.yml` | 60, 62, 63 | `ghcr.io/grove/pg-trickle/builder:*` → `ghcr.io/trickle-labs/pg-trickle/builder:*` |
| `.github/workflows/ci.yml` | 273–274, 343–344, 600–601 | Same builder image pull/tag commands (6 lines) |
| `.github/workflows/ghcr.yml` | 5, 41, 265, 267 | Comment, `IMAGE_NAME` uses `github.repository_owner` (auto-resolved after transfer ✓), but vendor label `org.opencontainers.image.vendor=grove` and docs URL are hardcoded |
| `.github/workflows/release.yml` | 211 | `REPO_URL` build-arg uses `${{ github.repository }}` (auto-resolved ✓) |

> **Note on `ghcr.yml` `IMAGE_NAME`:** The expression
> `${{ github.repository_owner }}/pg_trickle` resolves automatically after the
> transfer.  No change to the expression itself is required, but the hardcoded
> strings on lines 265 and 267 must be updated.

---

### 2.4 GitHub issue / community config

| File | Line(s) | Change needed |
|---|---|---|
| `.github/ISSUE_TEMPLATE/config.yml` | 4, 7 | `github.com/grove/pg-trickle/discussions` and `.../security/advisories/new` |

---

### 2.5 Documentation configuration

| File | Change needed |
|---|---|
| `book.toml` (lines 9–10) | `git-repository-url` and `edit-url-template` |
| `META.json` (lines 23, 25, 26, 30) | `homepage`, source `url`, source `web`, bug-tracker `web` |
| `ESSENCE.md` (line 196) | Source URL |
| `README.md` | PGXN overview document; replaces the retired `doc/pg_trickle.md` stub |

---

### 2.6 mdBook documentation (`docs/`)

The following files each contain one or more `github.com/grove/pg-trickle`
or `grove.github.io/pg-trickle` links that need replacement:

```
docs/ARCHITECTURE.md
docs/BENCHMARK.md
docs/CAPACITY_PLANNING.md
docs/CHANGELOG.md (pointer stub)
docs/CONFIGURATION.md
docs/contributing.md
docs/DEMO.md
docs/DVM_REWRITE_RULES.md
docs/ERRORS.md
docs/FAQ.md
docs/GETTING_STARTED.md
docs/GLOSSARY.md
docs/HA_AND_REPLICATION.md
docs/installation.md
docs/introduction.md
docs/PRE_DEPLOYMENT.md
docs/PROJECT_HISTORY.md
docs/QUICKSTART_5MIN.md
docs/RELEASE.md
docs/roadmap.md
docs/security.md
docs/SECURITY_GUIDE.md
docs/TROUBLESHOOTING.md
docs/UPGRADING.md
docs/integrations/citus.md
docs/integrations/dbt.md
docs/integrations/dbt-hub-submission.md
docs/tutorials/MIGRATING_FROM_PG_IVM.md
```

All occurrences follow two patterns:
- `https://github.com/grove/pg-trickle` → `https://github.com/trickle-labs/pg-trickle`
- `https://grove.github.io/pg-trickle/` → `https://trickle-labs.github.io/pg-trickle/`

---

### 2.7 Blog posts (`blog/`)

Every blog post (≈100 files) has an identical first line:

```markdown
[← Back to Blog Index](https://grove.github.io/pg-trickle/blog/) | [Documentation](https://grove.github.io/pg-trickle/)
```

Many also have footer attribution lines such as:

```markdown
*pg_trickle is an open-source PostgreSQL extension … at
[github.com/grove/pg-trickle](https://github.com/grove/pg-trickle).*
```

`blog/deploying-rag-at-scale.md` also references `ghcr.io/grove/pg-trickle-rag:latest`.

`blog/README.md` (line 11) references `grove.github.io/pg-trickle/`.

**Recommended approach:** Run a single `sed` / `find -exec sed` pass across
the entire `blog/` tree rather than editing every file individually.

---

### 2.8 dbt adapter

| File | Change needed |
|---|---|
| `dbt-pgtrickle/README.md` (lines 4, 27) | GitHub URL and `git:` package reference |
| `dbt-pgtrickle/AGENTS.md` (lines 6, 203) | GitHub URL |
| `examples/dbt_getting_started/packages.yml` (line 2) | `git:` package URL |
| `README.md` (line 485) | `git:` package URL in the dbt quick-start snippet |

---

### 2.9 Plans and roadmap docs

These files contain `grove/pg-trickle` references for historical context,
issue tracking, or comparison tables.  They are lower priority than the
user-facing files but should be updated for consistency:

| File | Notes |
|---|---|
| `plans/PLAN_RELAY_STANDALONE.md` (lines 164, 182, 244) | References `grove/pg-trickle-relay` (see §3.4) |
| [Historical pruning report at the v0.106.0 review](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/REPORT_SUPERFLOUS_FEATURES.md) | References `grove/pg-trickle-dbt`, `grove/pg-trickle-relay`, `grove/pg-trickle-observability` |
| `plans/ecosystem/PLAN_CLOUDNATIVEPG.md` (lines 6, 210, 514, 546) | PR link, ARG REPO_URL example, image refs |
| `plans/ecosystem/GAP_PG_IVM_COMPARISON.md` (lines 58, 919) | Comparison table repository column |
| `plans/ecosystem/REPORT_READYSET.md` (line 37) | Comparison table |
| `plans/ecosystem/PLAN_CITUS.md` (lines 32, 696) | Discussion links |
| `plans/dbt/PLAN_DBT_GETTING_STARTED_PROJECT.md` (lines 164, 638) | `git:` URL example, status table |
| `plans/safety/PLAN_FRONTIER_VISIBILITY_HOLDBACK.md` (line 5) | Issue link |
| `plans/infra/PLAN_CODECOV.md` (line 5) | Codecov dashboard URL |
| `roadmap/v0.4.0.md-full.md` (line 107) | Codecov dashboard URL |
| `roadmap/v0.14.0.md-full.md` (lines 60, 199) | GHCR image references |
| `roadmap/v0.43.0.md` (lines 78–79) | Issue / discussion links |
| `roadmap/v1.0.0.md-full.md` (lines 50, 67) | Cosign verify command referencing `grove/pg-trickle:1.0.0` |
| `AGENTS.md` (top of file) | References appear indirectly via workspace — review |
| `dbt-pgtrickle/AGENTS.md` (lines 6, 203) | See §2.8 |
| `CHANGELOG.md` (line 3251) | Historical GHCR image name in release notes |

> **Historical entries in `CHANGELOG.md` and `roadmap/v*.md-full.md`:** These
> record past state.  Updating them is optional (a case can be made to leave
> them as-is to preserve historical accuracy).

---

### 2.10 Docker Compose and playground

| File | Change needed |
|---|---|
| `demo/docker-compose.yml` (line 3) | `ghcr.io/grove/pg_trickle:latest` |
| `demo/README.md` (lines 45, 82) | Same image name in prose |
| `playground/docker-compose.yml` (line 3) | `ghcr.io/grove/pg_trickle:latest` |
| `docs/QUICKSTART_5MIN.md` (lines 12, 30) | Same image name |

---

### 2.11 README.md badges and install snippet

| Element | Change needed |
|---|---|
| Build badge (line 3) | Badge SVG URL and link → `trickle-labs/pg-trickle` |
| CI badge (line 4) | Same |
| Release badge (line 5) | Same |
| Coverage badge (line 6) | Codecov URL → `codecov.io/gh/trickle-labs/pg-trickle` |
| DeepWiki badge (line 12) | `deepwiki.com/grove/pg-trickle` → `deepwiki.com/trickle-labs/pg-trickle` |
| dbt `packages.yml` snippet (line 485) | `git:` URL |

---

### 2.12 Test infrastructure

| File | Change needed |
|---|---|
| `tests/build_e2e_image.sh` (line 29) | Comment referencing `ghcr.io/grove/pg-trickle/builder:pg18` |

---

## 3. Key Considerations and Risks

### 3.1 GitHub's automatic redirect

When a repository is transferred, GitHub creates a **301 redirect** from
`github.com/grove/pg-trickle` to `github.com/trickle-labs/pg-trickle`.

**What works automatically:**
- Browser navigation
- `git clone https://github.com/grove/pg-trickle` (HTTP redirect followed)
- Existing clone remote URLs (`git fetch`, `git push` still work)
- GitHub Actions badge URLs in external READMEs

**What does NOT work:**
- The redirect is **broken permanently** if anyone later creates a
  `grove/pg-trickle` repo (the redirect only survives while the slug is
  unclaimed).
- `ghcr.io` images do **not** redirect. Images at
  `ghcr.io/grove/pg_trickle` and `ghcr.io/grove/pg-trickle/builder` become
  frozen at the last published version and new images will be published
  under `ghcr.io/trickle-labs/`.
- GitHub Pages does **not** redirect. `grove.github.io/pg-trickle/` will 404
  immediately after transfer. All blog headers, documentation cross-links,
  and any external sites linking to the docs will break instantly.

**Mitigation:**
- Update all GitHub Pages URLs in the repo **before** or **on the same PR as**
  the transfer.
- Coordinate the DNS/Pages cutover carefully.
- After transfer, set up a minimal redirect-only GitHub Pages on the old org
  if you retain access to it (a single `index.html` with `<meta http-equiv="refresh">`).

---

### 3.2 GitHub Pages URL change

| Old URL | New URL |
|---|---|
| `https://grove.github.io/pg-trickle/` | `https://trickle-labs.github.io/pg-trickle/` |
| `https://grove.github.io/pg-trickle/blog/` | `https://trickle-labs.github.io/pg-trickle/blog/` |

`book.toml` does not embed the full hostname — `site-url = "/pg-trickle/"` is
path-relative.  The `docs.yml` blog build also uses `site-url = "/pg-trickle/blog/"`.
No change is needed in these expressions **if the repo name stays `pg-trickle`**.

The switch from `grove.github.io` to `trickle-labs.github.io` is purely a
consequence of which organisation hosts the Pages deployment.  No file change
is needed in `book.toml` or `docs.yml` as long as the repo name is unchanged.

**If a custom domain is configured later** (e.g. `docs.pgtrickle.dev`), the
`book.toml` `site-url` and blog `book.toml` would need updating at that time.

---

### 3.3 Docker Hub organisation (`pgtrickle`)

`.github/workflows/docker-hub.yml` targets the Docker Hub organisation
`pgtrickle` (not `grove`). It builds and smoke-tests the shared pinned
`Dockerfile.ghcr`. Docker Hub remains independent of the GitHub organisation.

If Docker Hub push is enabled in the future (currently gated until v1.0.0),
ensure `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets are present in the
new `trickle-labs` organisation.

---

### 3.4 Related planned repositories

`plans/PLAN_RELAY_STANDALONE.md` and the [historical pruning report at the
v0.106.0 review](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/REPORT_SUPERFLOUS_FEATURES.md)
reference future spin-off repositories:

- `grove/pg-trickle-relay`
- `grove/pg-trickle-dbt`
- `grove/pg-trickle-observability`

If these are ever created they should be created under `trickle-labs/` instead.
Update the plan documents to use `trickle-labs/` slugs.

---

### 3.5 GHCR builder image continuity

The CI workflows pull `ghcr.io/grove/pg-trickle/builder:pg18` as a cache
source.  After transfer:

1. The old image still exists at the old path (packages are not transferred).
2. The `builder-image.yml` workflow will push a new image to
   `ghcr.io/trickle-labs/pg-trickle/builder:pg18`.
3. Until at least one builder image has been pushed at the new path, CI will
   fall through to a cold build (slower but not broken — the workflow handles
   the `docker pull` failure gracefully with `2>/dev/null`).

**Action:** Run the `builder-image.yml` workflow manually immediately after the
org-level package visibility is configured in `trickle-labs`.

---

### 3.6 GitHub Actions secrets

All secrets stored in `grove/pg-trickle` must be re-added to the new
repository.  Repository transfer does **not** copy secrets.

| Secret | Used by | Action required |
|---|---|---|
| `CODECOV_TOKEN` | `coverage.yml` | Re-add after reconnecting Codecov to new repo |
| `PGXN_USERNAME` | `pgxn.yml`, `justfile` | Re-add |
| `PGXN_PASSWORD` | `pgxn.yml`, `justfile` | Re-add |
| `DOCKERHUB_USERNAME` | `docker-hub.yml` (future) | Re-add when enabling push |
| `DOCKERHUB_TOKEN` | `docker-hub.yml` (future) | Re-add when enabling push |

---

### 3.7 Codecov

Codecov links repositories via the GitHub App.  After transfer:

1. The Codecov GitHub App must be authorised for the `trickle-labs`
   organisation.
2. Codecov will create a new project at
   `app.codecov.io/github/trickle-labs/pg-trickle`.
3. The `CODECOV_TOKEN` secret must be re-added to the new repo (token is
   per-project).
4. The README coverage badge URL must be updated:
   - Old: `https://codecov.io/gh/grove/pg-trickle/branch/main/graph/badge.svg`
   - New: `https://codecov.io/gh/trickle-labs/pg-trickle/branch/main/graph/badge.svg`

---

### 3.8 DeepWiki

The DeepWiki badge in `README.md` currently points to
`https://deepwiki.com/grove/pg-trickle`.  DeepWiki must re-index the
repository at its new location.  Update the badge URL to
`https://deepwiki.com/trickle-labs/pg-trickle` and visit DeepWiki to trigger
re-indexing.

---

### 3.9 PGXN

PGXN packages are identified by their **PGXN account** (set via
`PGXN_USERNAME`), not the GitHub organisation.  No PGXN-side action is
required beyond updating `META.json`'s `homepage` field so future PGXN
distribution pages link to the right URL.

---

### 3.10 Branch protection rules and team permissions

GitHub transfers branch protection rules and repository settings, but
**team permissions are not transferred** when moving between organisations.
The `trickle-labs` organisation admin must re-configure:

- Required reviewers / CODEOWNERS
- Branch protection rules (these do transfer if both orgs use the same plan tier)
- Team memberships

---

### 3.11 Open issues and pull requests

All open issues and PRs move with the repository.  Their numbers are
preserved.  The embedded issue links in source code (e.g., `src/cdc.rs` line
716 references `#536`) will still resolve because the GitHub redirect is in
place.  These can be updated opportunistically.

---

### 3.12 External integrations

Check whether any of the following use the old repository slug and need
manual re-configuration:

- **Dependabot** — re-configure in `.github/dependabot.yml` if present; settings transfer
- **CodeQL** — `.github/workflows/codeql.yml` uses `github.repository` expressions; no code change needed
- **Semgrep** — `.github/workflows/semgrep.yml`; check for any hardcoded slug
- **Snyk / cargo-deny** — `deny.toml` has no org-specific references ✓
- **Any webhook or third-party CI** configured against the old repository URL

---

### 3.13 Git remote migration for contributors

Existing contributors' local clones will continue to work via the GitHub
redirect, but they should update their remotes:

```bash
git remote set-url origin https://github.com/trickle-labs/pg-trickle.git
```

Add a notice to `CONTRIBUTING.md` announcing the migration.

---

## 4. Recommended Migration Steps

### Phase 1 — Code changes (this PR and follow-on PRs, before transfer)

1. Update all `github.com/grove/pg-trickle` references in source code, Cargo
   manifests, Dockerfiles, and CI workflows to
   `github.com/trickle-labs/pg-trickle`.
2. Update all `ghcr.io/grove/` references (builder image, Dockerfiles,
   ci.yml) to `ghcr.io/trickle-labs/`.
3. Update all `grove.github.io/pg-trickle` references to
   `trickle-labs.github.io/pg-trickle`.
4. Update `META.json`, `book.toml`, `ESSENCE.md`, `README.md` badges.
5. Update all `blog/` headers and footers (bulk `sed` recommended).
6. Update `docs/` and `doc/` files.
7. Update dbt adapter files.
8. Update Docker Compose / playground files.

### Phase 2 — External service setup (before or concurrent with transfer)

1. Create `trickle-labs` GitHub organisation if it does not exist.
2. Authorise Codecov GitHub App for `trickle-labs`.
3. Obtain fresh `CODECOV_TOKEN` for the new project.
4. Prepare secrets list (PGXN credentials, future Docker Hub creds).

### Phase 3 — Repository transfer

1. Go to **Settings → Danger Zone → Transfer repository** in `grove/pg-trickle`.
2. Transfer to `trickle-labs`.
3. Immediately add all required secrets to the new repo.
4. Update git remotes on all developer machines.

### Phase 4 — Post-transfer verification

1. Trigger `builder-image.yml` manually to publish
   `ghcr.io/trickle-labs/pg-trickle/builder:pg18`.
2. Run a full CI pass (`gh workflow run ci.yml --ref main`).
3. Verify GitHub Pages deploys to `trickle-labs.github.io/pg-trickle/`.
4. Verify Codecov badge resolves.
5. Verify PGXN workflow still uploads correctly.
6. Update DeepWiki (visit `deepwiki.com` and trigger re-indexing).
7. Announce the migration to known users / downstream projects.

---

## 5. Quick-Reference: All Substitution Patterns

| Find | Replace |
|---|---|
| `https://github.com/grove/pg-trickle` | `https://github.com/trickle-labs/pg-trickle` |
| `https://grove.github.io/pg-trickle` | `https://trickle-labs.github.io/pg-trickle` |
| `ghcr.io/grove/pg_trickle` | `ghcr.io/trickle-labs/pg_trickle` |
| `ghcr.io/grove/pg-trickle/builder` | `ghcr.io/trickle-labs/pg-trickle/builder` |
| `ghcr.io/grove/pg-trickle-rag` | `ghcr.io/trickle-labs/pg-trickle-rag` |
| `codecov.io/gh/grove/pg-trickle` | `codecov.io/gh/trickle-labs/pg-trickle` |
| `app.codecov.io/github/grove/pg-trickle` | `app.codecov.io/github/trickle-labs/pg-trickle` |
| `deepwiki.com/grove/pg-trickle` | `deepwiki.com/trickle-labs/pg-trickle` |
| `org.opencontainers.image.vendor=grove` | `org.opencontainers.image.vendor=trickle-labs` |
| `grove/pg-trickle-relay` (planned repo) | `trickle-labs/pg-trickle-relay` |
| `grove/pg-trickle-dbt` (planned repo) | `trickle-labs/pg-trickle-dbt` |

> **Do not** do a global blind search-and-replace.  The string `grove` also
> appears in unrelated third-party project names (e.g., `sraoss/pg_ivm`
> comparison tables, MartenDB references in blog posts).  Prefer the full
> URL patterns listed above.

---

## 6. Estimated Effort

| Task | Effort |
|---|---|
| Source code + Cargo (4 files, ~8 occurrences) | 15 min |
| CI workflows + Dockerfiles (8 files) | 30 min |
| `META.json`, `book.toml`, `ESSENCE.md`, `doc/` | 15 min |
| `docs/` (~28 files, bulk sed) | 20 min |
| `blog/` (~100 files, bulk sed) | 10 min |
| dbt adapter + examples (4 files) | 10 min |
| `README.md` badges | 10 min |
| Docker Compose / playground (3 files) | 5 min |
| Plan/roadmap docs (~15 files) | 20 min |
| External service setup (Codecov, secrets, DeepWiki) | 1–2 h |
| Post-transfer verification | 30 min |
| **Total** | **~4 h** |

---

*Report generated by GitHub Copilot — 2026-05-06*
