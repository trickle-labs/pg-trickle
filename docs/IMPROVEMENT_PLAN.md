# Documentation Improvement Proposal

Generated: 2026-05-22  
Scope: Full `docs/` review — all 60+ files, integrations/, research/, tutorials/

---

## Summary

Full review of ~30 800 lines across 65+ markdown files. All relative Markdown
links resolve correctly (zero broken links). Findings: **2 critical**,
**5 major**, **6 minor**, **5 gaps**, and **3 structural recommendations**.
The most urgent item is the large version-history gap in `WHATS_NEW.md` (v0.34
→ v0.70 is undocumented). Several public SQL functions are documented only in
the auto-generated catalog and missing from the narrative `SQL_REFERENCE.md`.

---

## Critical (breaks user experience)

### C1 — `WHATS_NEW.md` has a 36-version gap

- **File:** docs/WHATS_NEW.md, line 10 (last entry: `## v0.34`)
- **Issue:** The extension is at v0.70.0 but `WHATS_NEW.md` stops at v0.34.
  Users upgrading across major feature boundaries have no human-readable
  summary of what changed. Thirty-six minor versions (v0.35–v0.70) are
  completely absent.
- **Fix:** Add section entries for at least the feature-significant releases
  between v0.34 and v0.70. Use the CHANGELOG as a data source. Even
  coarse groupings (e.g., "v0.40–v0.50 — DuckLake sink, columnar backends,
  parallel refresh maturation") would help.

### C2 — `pg_trickle.control` has a malformed description

- **File:** pg_trickle.control, line 1
- **Issue:** `comment = 'Streaming stream tables with differential view
  maintenance for PostgreSQL'` — "Streaming stream tables" is a noun phrase
  collision. `\l+` in psql and the PostgreSQL extension catalog both surface
  this comment, so it is the first thing many users see.
- **Fix:** Change to `'Self-maintaining stream tables with differential view
  maintenance for PostgreSQL'` (or similar non-redundant phrasing).

---

## Major (misleads or confuses)

### M1 — Several public SQL functions absent from `SQL_REFERENCE.md`

- **File:** docs/SQL_REFERENCE.md
- **Issue:** The following named `#[pg_extern]` functions exist in source and
  appear in `SQL_API_CATALOG.md` but have **no narrative entry** in
  `SQL_REFERENCE.md`:
  - `explain_diff_sql` — used in `PERFORMANCE_COOKBOOK.md` and `ERRORS.md`
    without a canonical reference
  - `reliability_counters` — referenced in `CONFIGURATION.md` (line ~1510)
  - `wal_source_status` — WAL CDC diagnostics
  - `worker_allocation_status` — worker pool diagnostics
  - `vector_status` — used in three tutorials
  - `st_auto_threshold` — threshold inspection utility
- **Fix:** Add a section for each function under the appropriate heading in
  `SQL_REFERENCE.md` (at minimum: signature, description, one example).

### M2 — `DVM_OPERATORS.md` contains a grammar error in a key definition

- **File:** docs/DVM_OPERATORS.md, line 48
- **Issue:** `"During an differential refresh…"` — `an` should be `a`.
  This appears in the core conceptual overview, immediately before the
  operator tree diagram.
- **Fix:** Change to `"During a differential refresh…"`.

### M3 — `GUC_CATALOG.md` does not mark `pg_trickle.unlogged_buffers` as deprecated

- **File:** docs/GUC_CATALOG.md, line 134
- **Issue:** The source code emits an explicit deprecation warning at runtime
  (`"pg_trickle.unlogged_buffers is deprecated. Use
  pg_trickle.change_buffer_durability instead."`), but the catalog row only
  says `"Default false — change buffers remain WAL-logged and crash-safe."` —
  no deprecation notice. Users who find the GUC in the catalog have no
  documentation cue to migrate.
- **Fix:** Append `**Deprecated** — use
  \`pg_trickle.change_buffer_durability\` instead.` to the description
  column in the auto-generated table; update the template in
  `scripts/gen_catalogs.py` so it is regenerated automatically.

### M4 — `DOCS_OWNERSHIP.md` and `DEPENDENCIES.md` are orphaned from SUMMARY.md

- **File:** docs/SUMMARY.md
- **Issue:** Two docs files are not linked in `SUMMARY.md`:
  - `docs/DEPENDENCIES.md` — contains the dependency policy for Rust crates
  - `docs/DOCS_OWNERSHIP.md` — describes canonical sources for each fact type
  Both are useful reference files that contributors and operators need to
  discover. Without a `SUMMARY.md` entry they are invisible in the rendered
  mdBook site.
- **Fix:** Add both under a `# Reference > Internals` or `# Contributing`
  section in `SUMMARY.md`.

### M5 — `QUICKSTART_5MIN.md` has no upfront prerequisites section

- **File:** docs/QUICKSTART_5MIN.md
- **Issue:** The quickstart jumps straight into Docker commands without a
  prerequisites block. Users who already have a PostgreSQL 18 server (i.e.,
  they skip to Step 2) may be missing `max_worker_processes = 32` or
  `shared_preload_libraries`, which causes silent scheduler failures.
  GETTING_STARTED.md explicitly calls this out (line ~65), but the quickstart
  does not.
- **Fix:** Add a one-table prerequisites block before Step 1:
  ```
  | Requirement | Notes |
  |---|---|
  | PostgreSQL 18.x | Required |
  | `shared_preload_libraries = 'pg_trickle'` | Must restart PG after changing |
  | `max_worker_processes ≥ 32` | Default 8 exhausted with multiple DBs |
  | `psql` or any SQL client | |
  ```
  Link to [Installation](installation.md) for full details. Mirrors the
  pattern already used in `tutorials/FIRST_DASHBOARD.md` and
  `GETTING_STARTED.md`.

---

## Minor (polish, consistency)

### m1 — `change-buffer` hyphenation is inconsistent in `research/` files

- **Files:** docs/research/multi_db_refresh_broker.md,
  docs/research/PG_IVM_COMPARISON.md, docs/research/TRIGGERS_VS_REPLICATION.md
- **Issue:** These files use `change-buffer` (hyphenated). The main docs
  consistently use `change buffer` (two words), which matches `GLOSSARY.md`.
- **Fix:** Normalise to `change buffer` (two words, no hyphen) throughout the
  research/ subdirectory.

### m2 — `ERRORS.md` log-line example uses `DIFF` capitalisation

- **File:** docs/ERRORS.md, line 937
- **Issue:** `"[pg_trickle] DIFF refresh for <table> took Xms vs last FULL
  Yms"` uses `DIFF` where every other doc uses `DIFFERENTIAL` or `differential`.
  In context this is fine (it matches the actual log output), but a reader
  scanning for `DIFFERENTIAL` will not find this example.
- **Fix:** Add a parenthetical note: `(abbreviated as 'DIFF' in log output)`.

### m3 — `DVM_OPERATORS.md` Prior Art section appears before Overview

- **File:** docs/DVM_OPERATORS.md, lines 37–46
- **Issue:** The Prior Art citations appear before the Overview section.
  Academic citations work better at the end of a document (or in a footnote
  appendix) where a reader arrives after understanding the content.
- **Fix:** Move the "Prior Art" section to the bottom of the document, after
  all operator sections.

### m4 — `STORAGE_BACKENDS.md` is in `SUMMARY.md` but not under any heading

- **File:** docs/SUMMARY.md
- **Issue:** `STORAGE_BACKENDS.md` is listed in the Build section without
  a header, so in the mdBook sidebar it appears flat among other pages rather
  than grouped. (Minor cosmetic.)
- **Fix:** Ensure it is listed under `# Build with Stream Tables` with the
  other reference docs.

### m5 — Tutorial `FIRST_DASHBOARD.md` prerequisites omit `max_worker_processes`

- **File:** docs/tutorials/FIRST_DASHBOARD.md, line 20
- **Issue:** The prerequisites block lists only PostgreSQL 18 and a SQL
  client. The `max_worker_processes` requirement that `GETTING_STARTED.md`
  calls critical is absent.
- **Fix:** Add `max_worker_processes ≥ 32` to the prerequisites, with a link
  to `INSTALL.md#postgresql-configuration`.

### m6 — `GUC_CATALOG.md` auto-comment says "129 GUCs" in some contexts

- **File:** docs/CONFIGURATION.md, line ~12 (coverage table)
- **Issue:** `CONFIGURATION.md` refers to "all 129 GUCs" in its coverage
  table while `GUC_CATALOG.md` header says **130 configuration parameters**.
  One is stale.
- **Fix:** Regenerate `GUC_CATALOG.md` via `python3 scripts/gen_catalogs.py`
  and confirm the CONFIGURATION.md cross-reference matches.

---

## Gaps (undocumented features or parameters)

### G1 — `explain_diff_sql()` has no entry in `SQL_REFERENCE.md`

- **Missing from:** docs/SQL_REFERENCE.md
- **Source:** src/monitor/mod.rs:1092
- **Notes:** Used in three production-facing docs without a reference target.
  See also M1 above.

### G2 — `reliability_counters()` has no entry in `SQL_REFERENCE.md`

- **Missing from:** docs/SQL_REFERENCE.md
- **Source:** src/monitor/mod.rs:692
- **Notes:** Referenced in `CONFIGURATION.md` as a monitoring primitive.

### G3 — `wal_source_status()`, `worker_allocation_status()`, `vector_status()`

- **Missing from:** docs/SQL_REFERENCE.md
- **Sources:** src/monitor/health.rs:974, src/api/diagnostics.rs, src/monitor/mod.rs
- **Notes:** All three appear in tutorials and `SQL_API_CATALOG.md` but lack
  narrative documentation (return-column descriptions, usage context, examples).

### G4 — `st_auto_threshold()` behaviour is not documented anywhere

- **Missing from:** docs/SQL_REFERENCE.md, docs/CONFIGURATION.md
- **Source:** src/monitor/mod.rs:583
- **Notes:** Its relationship to `pg_trickle.differential_max_change_ratio`
  (returns per-ST override if set, otherwise global GUC) is described only in
  `SQL_API_CATALOG.md` in a single line. Operators tuning auto-thresholds
  per stream table have nowhere to find this.

### G5 — Versions v0.35–v0.69 are undocumented in `WHATS_NEW.md`

- **Missing from:** docs/WHATS_NEW.md
- **Notes:** See C1. Flagged here as a content gap as well as a critical UX
  issue — the changelog (`CHANGELOG.md`) is exhaustive but not human-curated;
  `WHATS_NEW.md` is the human-readable layer that is currently 36 versions
  out of date.

---

## Recommendations (structural)

### R1 — Add a "What's new in v0.70" section to `WHATS_NEW.md` immediately

- **Rationale:** v0.70.0 is the current release. Every new user lands on docs
  that present v0.34 as the most recent version, which signals an abandoned
  project. Even a one-paragraph entry for v0.70 would close the perception gap.
- **Action:** Write `## v0.70 — <headline> (May 2026)` with three to five
  bullet points pulled from the CHANGELOG. Then backfill v0.35–v0.69 as time
  allows.

### R2 — Establish a "SQL Reference completeness gate" in CI

- **Rationale:** Functions `explain_diff_sql`, `reliability_counters`,
  `wal_source_status`, `worker_allocation_status`, `vector_status`,
  `st_auto_threshold` all exist in `SQL_API_CATALOG.md` (auto-generated) but
  are absent from `SQL_REFERENCE.md` (hand-written). The divergence will grow
  unless there is a CI check.
- **Action:** Extend `scripts/gen_catalogs.py` (or add a new lint script) to
  verify that every named `#[pg_extern]` function that is not marked
  `sql = false` appears in `SQL_REFERENCE.md`. Fail CI if coverage drops below
  100%.

### R3 — Create a top-level "Operator & DBA Quick Reference" card

- **Rationale:** Operators tuning pg_trickle in production currently navigate
  across `CONFIGURATION.md`, `GUC_CATALOG.md`, `PERFORMANCE_CHEATSHEET.md`,
  `TROUBLESHOOTING.md`, and `PRE_DEPLOYMENT.md`. A single "top 10 commands
  and GUCs" card on one page (similar to `PERFORMANCE_CHEATSHEET.md` but
  operations-focused) would reduce first-day friction.
- **Action:** Create `docs/OPS_CHEATSHEET.md` that collects the most-used
  diagnostic queries, the five most important GUCs for production, and links
  to each detailed reference. Add to `SUMMARY.md` under `# Operate`.
