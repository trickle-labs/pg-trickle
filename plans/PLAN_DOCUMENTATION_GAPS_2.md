# Plan: Documentation Gap Analysis — Round 2

**Date:** 2026-05-11
**Status:** PROPOSED — approved for implementation in v0.56.0 and v0.57.0
**Scope:** Full audit of `docs/`, `docs/tutorials/`, `docs/integrations/`,
`docs/research/`, `docs/GUC_CATALOG.md`, `docs/ERRORS.md`, and all root-level
documentation (`README.md`, `INSTALL.md`, `CHANGELOG.md`, `CONTRIBUTING.md`,
`SECURITY.md`, `ROADMAP.md`, `ESSENCE.md`). Cross-referenced against source
code in `src/api/`, `src/config.rs`, and `src/error.rs`.

**Author audience:** maintainers and documentation owners.
**Reader target:** application developers, data engineers, DBAs, SREs,
evaluators — ranging from SQL-literate analysts to senior platform engineers.
**Predecessor:** [Round 1 report at the v0.106.0 review](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/PLAN_DOCUMENTATION_GAPS_1.md)
(2026-04-27; it covered structural stubs, SUMMARY.md, the glossary, and the
use-case gallery).

---

## 1. Executive Summary

Round 2 follows the structural work of Round 1 with a **deep accuracy and
completeness audit** of every documentation file, cross-referenced against
source code. The audit covered 80+ files, ~32,000 lines of documentation,
59 SQL-callable functions, 48+ GUC variables, and 44 error variants.

The overall documentation quality is high — the reference material
(`SQL_REFERENCE.md`, `CONFIGURATION.md`, `ARCHITECTURE.md`,
`TROUBLESHOOTING.md`) is production-grade, the tutorial set is thorough,
and the integration guides are practical. Three issues, however, require
attention before v1.0:

1. **`docs/GUC_CATALOG.md` is corrupted** — the code generator produces
   `(registration pending — PGS_*)` placeholder names for every entry,
   raw Rust types instead of PostgreSQL types, and four malformed duplicate
   rows at the end. Users relying on this file as a GUC quick-reference
   currently see garbage.

2. **`docs/ERRORS.md` is 54% complete** — only 7 of 13 error categories in
   `src/error.rs` are documented. The six missing categories (Publication,
   SLA, Snapshot, Outbox, Placeholder validation, DVM hardening) include
   errors that users will encounter in v0.28+ deployments.

3. **Seven new documents are missing entirely** — `LIMITATIONS.md`,
   `MENTAL_MODEL.md`, `PERFORMANCE_CHEATSHEET.md`, and four tutorials
   (first dashboard, event sourcing, backfill/migration, security
   hardening) are called for in the PRD but do not yet exist.

Beyond these three, the audit surfaced 17 additional items across accuracy,
completeness, structure, and polish. All 20 items are sequenced across two
releases: v0.56.0 (foundation — P0 and P1) and v0.57.0 (excellence — P2/P3,
new docs, consistency pass).

---

## 2. Methodology

The audit proceeded in five passes:

1. **Structure pass** — enumerated all 80+ files; checked mdBook SUMMARY.md
   coverage; verified cross-link targets.
2. **Source cross-reference** — read `src/api/mod.rs`, all files under
   `src/api/`, `src/config.rs`, and `src/error.rs`; catalogued all
   `#[pg_extern]` functions, all GUC definitions, and all error variants.
3. **Completeness pass** — compared the source inventory against
   `docs/SQL_REFERENCE.md`, `docs/CONFIGURATION.md`,
   `docs/GUC_CATALOG.md`, and `docs/ERRORS.md`.
4. **Accuracy pass** — checked GUC defaults, function signatures,
   schema names, and code examples against source truth.
5. **Quality pass** — sampled prose, examples, structure, and
   cross-links across all tutorial and integration files.

---

## 3. Full Inventory of Audit Findings

### 3.1 Source Surface (audited)

| Category | Count |
|----------|-------|
| SQL-callable functions (`#[pg_extern]`) | 59 |
| GUC variables (`src/config.rs`) | 48 |
| Error variants (`src/error.rs`) | 44 |
| Documentation files audited | 83 |
| Total documentation lines | ~32,000 |

### 3.2 GUC_CATALOG.md — Critical Corruption

The auto-generator (`scripts/gen_catalogs.py`) has a bug where it exposes
internal C-macro names instead of the resolved PostgreSQL GUC names. Every
row in the table looks like:

```
| `(registration pending — PGS_ENABLED)` | `bool` | `true` | … |
```

instead of:

```
| `pg_trickle.enabled` | `bool` | `true` | … |
```

Additional problems:
- Rust types (`Option<std::ffi::CString>`) appear instead of SQL types
  (`text`).
- The final four rows are malformed duplicates of `pg_trickle.enabled`
  with wrong types and defaults (`i32 / 256`, `i32 / 128`, `bool / false`,
  `f64 / 0.20`).
- The header claims "115 parameters" but the table has more rows, many
  duplicated.

**Impact:** Any user consulting GUC_CATALOG.md as a quick reference gets
completely wrong information. This is a P0 blocker.

### 3.3 ERRORS.md — Missing Error Categories

`src/error.rs` defines 44 error variants across 14 logical groups.
`docs/ERRORS.md` documents variants from only 8 of those groups:

| Group | Status |
|-------|--------|
| User errors (8 variants) | ✅ Documented |
| Schema errors (2 variants) | ✅ Documented |
| System errors (5 variants) | ✅ Documented |
| Watermark errors (3 variants) | ✅ Documented |
| Transient errors (1 variant) | ✅ Documented |
| Internal errors (1 variant) | ✅ Documented |
| DVM hardening (2 alerts) | ✅ Partial (alerts only, not error variants) |
| Publication errors (2 variants) | ❌ **Missing** |
| SLA errors (1 variant) | ❌ **Missing** |
| CDC errors (2 variants) | ❌ **Missing** |
| Diagnostic errors (1 variant) | ❌ **Missing** |
| Snapshot errors (3 variants) | ❌ **Missing** |
| Outbox errors (3 variants) | ❌ **Missing** |
| Placeholder validation errors (1 variant) | ❌ **Missing** |

Users operating v0.27+ stream tables (snapshots), v0.28+ outbox/inbox
deployments, or any deployment with WAL-based CDC will encounter undocumented
error variants.

### 3.4 CONFIGURATION.md — Default Value Conflict

`pg_trickle.parallel_refresh_mode` is documented with default `'off'` in
the prose and the sample `postgresql.conf` block, but `src/config.rs` and
`docs/GUC_CATALOG.md` (GUC_CATALOG shows `"on"` in description text) suggest
the current default is `'on'`. The source is authoritative; the docs need
correction.

### 3.5 SQL_REFERENCE.md — Incomplete Examples and Tables

The SQL reference is excellent overall (50+ functions, 70+ examples). Two
gaps:

**Missing examples (10 functions):** The v0.28.0 outbox consumer and inbox
API functions are documented with parameter lists but no examples:
`poll_outbox`, `commit_offset`, `extend_lease`, `seek_offset`,
`consumer_heartbeat`, `consumer_lag`, `drop_consumer_group`,
`outbox_rows_consumed`, `replay_inbox_messages`, `inbox_ordering_gaps`.

**Undocumented catalog tables (7 tables):** The following catalog tables are
referenced in prose but have no column-schema table:
`pgt_outbox_config`, `pgt_consumer_groups`, `pgt_consumer_offsets`,
`pgt_consumer_leases`, `pgt_inbox_config`, `pgt_inbox_ordering_config`,
`pgt_inbox_priority_config`.

### 3.6 Research Stubs — Three Files Are 15 Lines Each

Three of seven `docs/research/` files contain only an `{{#include}}` directive
pointing to `plans/` files:

| Stub | Points to |
|------|-----------|
| `docs/research/CUSTOM_SQL_SYNTAX.md` | `plans/sql/REPORT_CUSTOM_SQL_SYNTAX.md` |
| `docs/research/PG_IVM_COMPARISON.md` | `plans/ecosystem/GAP_PG_IVM_COMPARISON.md` |
| `docs/research/TRIGGERS_VS_REPLICATION.md` | `plans/sql/REPORT_TRIGGERS_VS_REPLICATION.md` |

A reader landing on these pages via the mdBook site or GitHub sees an empty
or near-empty page. The plans files may not be present, may be stale, or may
contain internal planning language unsuitable for user-facing docs. Each
needs a standalone summary added before the include.

### 3.7 DVM_REWRITE_RULES.md — Too Terse

`docs/DVM_REWRITE_RULES.md` is 180 lines for five rewrite passes
(view inlining, GROUPING SETS expansion, EXISTS-to-join rewrite, scalar
subquery hoisting, delta key restriction). Each pass is described in two to
four sentences with no worked SQL example. Given that these rewrites
determine whether a query is accepted by the DVM engine, users writing
complex queries need to understand what transformations occur. At minimum,
each pass requires a before/after SQL example.

### 3.8 introduction.md — Too Minimal

`docs/introduction.md` is 82 lines: one code example, a persona routing
table, and a footer. It has no INSTALL.md link in the persona table (DBA/SRE
path skips straight to PRE_DEPLOYMENT.md), no conceptual context beyond the
code snippet, and no explanation of what problem pg_trickle solves for a
reader who has not yet read ESSENCE.md.

### 3.9 Tutorial Gaps

**HYBRID_SEARCH_PATTERNS.md** — Pattern 1 (flat corpus) is fully worked;
patterns 2 (RLS-scoped) and 3 (tiered storage) are code sketches without
step-by-step instructions or expected output.

**PER_TENANT_ANN_PATTERNS.md** — Uses undocumented syntax
`partition_key => 'HASH:tenant_id:16'` in pattern 2. Patterns 2–3 lack
the detail of pattern 1.

**VECTOR_RAG_STARTER.md** — Calls `embedding_stream_table()` without
documenting its parameters inline or linking to SQL_REFERENCE.

**tuning-refresh-mode.md** — Composite score thresholds (+0.15 for
DIFFERENTIAL, −0.15 for FULL) appear in examples but are never stated in prose.

### 3.10 Security Documentation Gaps

**SECURITY_GUIDE.md** — The guide lists recommended privilege separation
practices but provides no concrete SQL templates for creating the described
roles and granting the described permissions. Users must infer the exact
GRANT statements from the prose description.

**SECURITY_MODEL.md** — The supply-chain section (v1.0 checklist) has several
TODO items that should either be filled in or explicitly deferred to v1.0.

### 3.11 Missing New Documents

The following documents are called for (in the original PRD scope) but do
not yet exist:

| File | Description |
|------|-------------|
| `docs/LIMITATIONS.md` | Unsupported query patterns, anti-patterns, "will this work?" decision tree |
| `docs/MENTAL_MODEL.md` | Concept-first DVM explainer for developers who know SQL but not IVM |
| `docs/PERFORMANCE_CHEATSHEET.md` | Single-page quick reference: top 10 configuration and query patterns |
| `docs/tutorials/FIRST_DASHBOARD.md` | Tutorial: real-time analytics dashboard with sample data and Grafana pointers |
| `docs/tutorials/EVENT_SOURCING.md` | Tutorial: stream tables as event-sourced read-model projections |
| `docs/tutorials/BACKFILL_AND_MIGRATION.md` | Tutorial: migrate a manually-maintained matview to a stream table |
| `docs/tutorials/SECURITY_HARDENING.md` | Step-by-step security hardening guide with role separation SQL |

### 3.12 Minor Issues (P2/P3)

- **WHATS_NEW.md** — entries for v0.1–v0.7 are stubs.
- **FAQ.md** — several cross-references use plain text instead of markdown
  links; some GUC names not linked to CONFIGURATION.md.
- **DVM_OPERATORS.md** — no quick-reference index at top of 3,200-line
  document.
- **PERFORMANCE_COOKBOOK.md §13** — DVM complexity tuning section is dense
  and lacks concrete before/after query examples.
- **Link inconsistencies** — `installation.md` referenced where `INSTALL.md`
  is meant in QUICKSTART_5MIN.md and GETTING_STARTED.md.
- **multi_db_refresh_broker.md** — implementation status unclear (design doc
  with unchecked TODO list and no linked tracking issue).

---

## 4. P0 — Blockers

Items that produce incorrect or misleading information for current users.

### DOC-P0-1: Regenerate GUC_CATALOG.md

**File:** `docs/GUC_CATALOG.md`
**Problem:** All GUC names appear as `(registration pending — PGS_*)`.
Rust types instead of PostgreSQL types. Four malformed trailing rows.
**Fix:** Fix `scripts/gen_catalogs.py` to:
  1. Resolve `PGS_*` macro names to their `pg_trickle.*` GUC names.
  2. Convert Rust types (`bool`, `i32`, `f64`, `Option<CString>`) to
     PostgreSQL type names (`bool`, `integer`, `float`, `text`).
  3. Remove the four trailing malformed rows.
  4. Re-run generation and commit the corrected file.
**Priority:** P0 — CRITICAL
**Effort:** Medium (generator fix + regeneration)

### DOC-P0-2: Complete ERRORS.md

**File:** `docs/ERRORS.md`
**Problem:** Six of 14 error categories in `src/error.rs` are
undocumented. Users of v0.27+ (snapshots), v0.28+ (outbox/inbox), or
WAL CDC will encounter undocumented errors.
**Fix:** Add sections for each missing group following the existing
template (message format, description, common causes, suggested fix):
  - Publication errors: `PublicationAlreadyExists`, `PublicationNotFound`
  - SLA errors: `SlaTooSmall`
  - CDC errors: `ChangedColsBitmaskFailed`, `PublicationRebuildFailed`
  - Diagnostic errors: `DiagnosticError`
  - Snapshot errors: `SnapshotAlreadyExists`, `SnapshotSourceNotFound`,
    `SnapshotSchemaVersionMismatch`
  - Outbox errors: `OutboxAlreadyEnabled`, `OutboxNotEnabled`,
    `PgTideMissing`
  - Placeholder validation: `UnresolvedPlaceholder`
  - DVM hardening: `DiffDepthExceeded`, `DiffCteCountExceeded`,
    `StSourceFrontierMissing`
**Priority:** P0 — CRITICAL
**Effort:** Large (12+ new error entries with full documentation)

### DOC-P0-3: Fix parallel_refresh_mode Default

**File:** `docs/CONFIGURATION.md`
**Problem:** `pg_trickle.parallel_refresh_mode` documented default is
`'off'`; source code default is `'on'`. Operators setting this GUC are
working from incorrect documentation.
**Fix:** Verify current default in `src/config.rs` and update the prose,
the default table, and the sample `postgresql.conf` block to match.
**Priority:** P0 — CRITICAL
**Effort:** Small

---

## 5. P1 — High Impact

Items that create significant friction for active users.

### DOC-P1-4: Add Missing SQL_REFERENCE Examples (Outbox/Inbox Consumer APIs)

**File:** `docs/SQL_REFERENCE.md`
**Problem:** Ten v0.28.0 consumer API functions have no usage examples:
`poll_outbox`, `commit_offset`, `extend_lease`, `seek_offset`,
`consumer_heartbeat`, `consumer_lag`, `drop_consumer_group`,
`outbox_rows_consumed`, `replay_inbox_messages`, `inbox_ordering_gaps`.
**Fix:** Add a minimal working example for each function. A "consumer
poll loop" recipe that chains `poll_outbox → commit_offset → extend_lease`
would cover several at once.
**Priority:** P1 — HIGH
**Effort:** Medium

### DOC-P1-5: Document Outbox/Inbox Catalog Table Schemas

**File:** `docs/SQL_REFERENCE.md`
**Problem:** Seven catalog tables are mentioned in prose but have no
column-schema documentation: `pgt_outbox_config`, `pgt_consumer_groups`,
`pgt_consumer_offsets`, `pgt_consumer_leases`, `pgt_inbox_config`,
`pgt_inbox_ordering_config`, `pgt_inbox_priority_config`.
**Fix:** Add a column table for each using the same format as the
existing catalog table documentation. Derive column names and types from
the SQL migration files in `sql/`.
**Priority:** P1 — HIGH
**Effort:** Medium

### DOC-P1-6: Resolve Research Stubs

**Files:** `docs/research/CUSTOM_SQL_SYNTAX.md`,
`docs/research/PG_IVM_COMPARISON.md`,
`docs/research/TRIGGERS_VS_REPLICATION.md`
**Problem:** Three files are 15-line stubs containing only `{{#include}}`
directives. Standalone reading (GitHub, direct URL) produces an empty or
broken page.
**Fix:** For each file, add a 2–3 paragraph abstract summarising the key
findings before the include directive. The abstract must be self-contained
so GitHub readers get the essential conclusion without following the link.
Verify that the referenced `plans/` files actually exist at the stated
paths.
**Priority:** P1 — HIGH
**Effort:** Medium

### DOC-P1-7: Expand DVM_REWRITE_RULES.md

**File:** `docs/DVM_REWRITE_RULES.md`
**Problem:** 180 lines for five complex rewrite passes — no worked SQL
examples. Users debugging unexpected DVM rejections cannot determine which
pass transformed (or rejected) their query.
**Fix:** Add a before/after SQL example for each of the five passes:
  1. View inlining — show a defining-query reference to a view, and the
     inlined equivalent.
  2. GROUPING SETS expansion — ROLLUP → equivalent UNION ALL.
  3. EXISTS-to-join rewrite — `WHERE EXISTS (SELECT …)` → `SEMI JOIN`.
  4. Scalar subquery hoisting — correlated scalar subquery lifted to CTE.
  5. Delta key restriction — output key columns added to WHERE clause of
     delta scan.
Each example should include: original SQL → transformed SQL → one sentence
on why the transformation is needed for differential maintenance.
**Priority:** P1 — HIGH
**Effort:** Medium

### DOC-P1-8: Enrich introduction.md

**File:** `docs/introduction.md`
**Problem:** 82 lines; no INSTALL.md link; no conceptual context beyond
a code snippet; persona table omits the installation step for all paths.
**Fix:**
  1. Add 2–3 paragraphs: (a) what problem pg_trickle solves in plain
     language, (b) how it works at one level of abstraction (trigger →
     change buffer → differential merge), (c) when not to use it (link
     to LIMITATIONS.md once created).
  2. Add an "Install" row or entry to the persona routing table that
     routes to INSTALL.md / QUICKSTART_5MIN.md.
  3. Keep the file under 200 lines — it is a hub, not a guide.
**Priority:** P1 — HIGH
**Effort:** Small

### DOC-P1-9: Create docs/MENTAL_MODEL.md

**File:** `docs/MENTAL_MODEL.md` (new)
**Description:** A concept-first explainer of differential view
maintenance aimed at a developer who knows SQL and materialized views but
has never encountered IVM.
**Structure:**
  1. The problem with `REFRESH MATERIALIZED VIEW CONCURRENTLY`
  2. The naive fix — and why it doesn't scale
  3. Insight: only the *changed rows* matter (delta thinking)
  4. How pg_trickle captures changes (CDC, change buffer)
  5. The differential merge — turning deltas into table updates
  6. Why some queries cannot be maintained differentially (and the FULL
     fallback)
  7. The DAG — maintaining chains of dependent views
  8. Further reading (ARCHITECTURE.md, DVM_OPERATORS.md, DBSP_COMPARISON.md)
**Tone:** Analogies and intuition before formulas. Active voice.
No DBSP/DRed jargon until §8 with links to the glossary.
**Priority:** P1 — HIGH
**Effort:** Large

### DOC-P1-10: Create docs/LIMITATIONS.md

**File:** `docs/LIMITATIONS.md` (new)
**Description:** An honest, well-organised reference for what pg_trickle
cannot do, will not do in DIFFERENTIAL mode, and is known to do poorly.
**Structure:**
  1. Unsupported SQL constructs (TABLESAMPLE, FOR UPDATE, volatile
     functions, mutable CTEs, SET RETURNING functions in FROM) —
     why each is excluded.
  2. DIFFERENTIAL-mode constraints (patterns that always fall back to
     FULL: certain DISTINCT, non-monotone aggregates, etc.) — with
     the workaround for each.
  3. Source table constraints (TRUNCATE semantics, unlogged tables,
     foreign tables without polling) — and their mitigations.
  4. Operational anti-patterns (defining queries with side effects,
     writing to stream tables directly, RLS bypass during refresh,
     high-cardinality IMMEDIATE mode).
  5. "Will this work?" decision tree — a flowchart-style checklist for
     evaluating whether a specific query is a good candidate.
  6. Known performance ceilings and the benchmarks that characterise them.
**Tone:** Direct and honest. No marketing language. Cross-link to
ARCHITECTURE.md, DVM_OPERATORS.md, and the relevant tutorial for each
limitation.
**Priority:** P1 — HIGH
**Effort:** Large

---

## 6. P2 — Medium Impact

### DOC-P2-11: Add Role/Grant SQL Templates to SECURITY_GUIDE.md

**File:** `docs/SECURITY_GUIDE.md`
**Problem:** Recommended role separation described in prose but no SQL.
**Fix:** Add a "Copy-Paste Templates" section with:
  - `CREATE ROLE` statements for `pgtrickle_admin`, `pgtrickle_user`,
    `pgtrickle_readonly`.
  - `GRANT` statements to assign minimum required privileges.
  - A note on ownership requirements for stream table schemas.
**Priority:** P2 — MEDIUM
**Effort:** Small

### DOC-P2-12: Backfill WHATS_NEW.md for v0.1–v0.7

**File:** `docs/WHATS_NEW.md`
**Problem:** Entries for v0.1–v0.7 are one-line stubs.
**Fix:** Write 3–5 sentence summaries for each release using the
CHANGELOG.md entries as source. Focus on user-visible impact, not
internal changes.
**Priority:** P2 — MEDIUM
**Effort:** Medium

### DOC-P2-13: Expand HYBRID_SEARCH_PATTERNS.md Patterns 2–3

**File:** `docs/tutorials/HYBRID_SEARCH_PATTERNS.md`
**Problem:** Patterns 2 (RLS-scoped corpus) and 3 (tiered storage) are
code sketches without step-by-step instructions, prerequisites, or
expected output.
**Fix:** Expand each to match the quality of pattern 1: add a goal
statement, prerequisites, step-by-step SQL, expected output, and a
"what's next" link. For pattern 2 specifically, document the
`pg_trickle.enable_vector_agg` GUC and when to enable it.
**Priority:** P2 — MEDIUM
**Effort:** Medium

### DOC-P2-14: Document partition_key Syntax in PER_TENANT_ANN_PATTERNS.md

**File:** `docs/tutorials/PER_TENANT_ANN_PATTERNS.md`
**Problem:** Pattern 2 uses `partition_key => 'HASH:tenant_id:16'` without
explanation. Patterns 2–3 are skeletal.
**Fix:** Add an inline explanation of the `partition_key` parameter syntax
(`HASH:<column>:<bucket_count>`). Expand patterns 2 and 3 with worked
examples.
**Priority:** P2 — MEDIUM
**Effort:** Medium

### DOC-P2-15: Fix Link Inconsistencies

**Files:** `docs/QUICKSTART_5MIN.md`, `docs/GETTING_STARTED.md`
**Problem:** Both files reference `installation.md` (lowercase) where
`INSTALL.md` (uppercase, root-level) is meant.
**Fix:** Change all `installation.md` occurrences in these two files to
the correct relative path `../INSTALL.md` (or just `INSTALL.md` from the
docs root).
**Priority:** P2 — MEDIUM
**Effort:** Small

### DOC-P2-16: Expand PERFORMANCE_COOKBOOK.md Section 13

**File:** `docs/PERFORMANCE_COOKBOOK.md`
**Problem:** Section 13 (DVM complexity) refers to TPC-H benchmarks for
context but gives no concrete before/after diagnostic examples.
**Fix:** Add 2–3 worked examples: (a) a query with too many CTEs hitting
`max_diff_ctes` and the tuning steps; (b) a query where switching to FULL
is faster than DIFFERENTIAL and how to detect this via
`recommend_refresh_mode()`; (c) a deep-join chain and how
`max_differential_joins` controls it.
**Priority:** P2 — MEDIUM
**Effort:** Small

### DOC-P2-17: Fill SECURITY_MODEL.md Supply-Chain TODOs

**File:** `docs/SECURITY_MODEL.md`
**Problem:** The v1.0 supply-chain checklist has several items still
marked TODO (signed release artifacts, SBOM generation, reproducible
builds).
**Fix:** For each item, either fill in the current implementation status
(e.g., "CI now signs artifacts via `cosign`") or mark explicitly as
"Planned for v1.0" with a link to the relevant roadmap milestone.
**Priority:** P2 — MEDIUM
**Effort:** Small

---

## 7. P3 — Low Impact (Polish)

### DOC-P3-18: Convert Plain-Text Cross-References in FAQ.md

**File:** `docs/FAQ.md`
**Problem:** Several answers reference GUC names and function names in
plain text where a `[link](target)` would aid navigation.
**Fix:** Convert the most common plain-text references to markdown links
pointing to CONFIGURATION.md and SQL_REFERENCE.md anchors.
**Priority:** P3 — LOW
**Effort:** Small

### DOC-P3-19: Add Quick-Reference Index to DVM_OPERATORS.md

**File:** `docs/DVM_OPERATORS.md`
**Problem:** 3,200-line document with no summary table at the top.
**Fix:** Add an operator quick-reference table at the start with:
  operator name, supported refresh modes (FULL/DIFF/IMMD), and a link
  to the detailed section.
**Priority:** P3 — LOW
**Effort:** Small

### DOC-P3-20: Document embedding_stream_table() Parameters in VECTOR_RAG_STARTER.md

**File:** `docs/tutorials/VECTOR_RAG_STARTER.md`
**Problem:** `embedding_stream_table()` is called without inline
parameter documentation or a link to SQL_REFERENCE.
**Fix:** Add a parameter breakdown table inline, or add a "See also:
`pgtrickle.embedding_stream_table()`" link with the relevant anchor.
**Priority:** P3 — LOW
**Effort:** Small

### DOC-P3-21: Explain Composite Score Thresholds in tuning-refresh-mode.md

**File:** `docs/tutorials/tuning-refresh-mode.md`
**Problem:** The +0.15 / −0.15 composite score thresholds appear in
examples but are never explained in prose.
**Fix:** Add a paragraph explaining the scoring formula and what the
threshold values represent.
**Priority:** P3 — LOW
**Effort:** Small

### DOC-P3-22: Clarify multi_db_refresh_broker.md Implementation Status

**File:** `docs/research/multi_db_refresh_broker.md`
**Problem:** The document is a design spec with an unchecked TODO list;
current implementation status is unknown.
**Fix:** Add a status banner at the top: either "Design phase —
implementation planned for v0.X.0 ([tracking issue #NNN](…))" or
"Implemented in v0.X.0 — see [MULTI_DATABASE.md](../MULTI_DATABASE.md)".
**Priority:** P3 — LOW
**Effort:** Small

---

## 8. Phase 4 — New Documents

### DOC-NEW-23: Create docs/PERFORMANCE_CHEATSHEET.md

**File:** `docs/PERFORMANCE_CHEATSHEET.md` (new)
**Description:** A single-page quick reference for the 10 most impactful
configuration changes and query patterns for performance-sensitive
deployments. Intended as a print/bookmark-friendly complement to the
1,500-line PERFORMANCE_COOKBOOK.md.
**Structure:**
  - One table of 10 GUC quick-wins with the recommended value and
    expected impact.
  - One table of 5 query patterns that trigger FULL fallback, with the
    rewrite that avoids them.
  - Three "golden rules" in a callout box.
  - Links to deeper sections of PERFORMANCE_COOKBOOK.md for each item.
**Priority:** P1 — HIGH
**Effort:** Medium

### DOC-NEW-24: Create docs/tutorials/FIRST_DASHBOARD.md

**File:** `docs/tutorials/FIRST_DASHBOARD.md` (new)
**Description:** Tutorial — build the backend for a real-time analytics
dashboard using stream tables over a sample e-commerce dataset. Covers:
creating source tables, defining aggregate stream tables, querying for
dashboard consumption, and connecting to Grafana / Metabase.
**Structure (standard tutorial format):**
  - Goal and prerequisites
  - Step 1: Create sample order and product data
  - Step 2: Create revenue-by-region and hourly-order-count stream tables
  - Step 3: Chain a "top products" stream table from the base aggregates
  - Step 4: Query patterns for a live dashboard (low-latency SELECT)
  - Step 5 (optional): Grafana data source configuration
  - Expected output at each step
  - What's next (OUTBOX.md for event publication, integrations/prometheus.md)
**Priority:** P1 — HIGH
**Effort:** Large

### DOC-NEW-25: Create docs/tutorials/EVENT_SOURCING.md

**File:** `docs/tutorials/EVENT_SOURCING.md` (new)
**Description:** Tutorial — use stream tables as read-model projections
over an event-sourced write model. Models an order-processing domain:
events table → aggregate stream tables (order state, customer totals,
inventory counters).
**Structure:**
  - Goal and prerequisites
  - Step 1: Create the events table (immutable, append-only)
  - Step 2: Define stream tables as read models (current order state,
    customer lifetime value, inventory levels)
  - Step 3: Demonstrate projection correctness across INSERT / UPDATE /
    DELETE on the events table
  - Step 4: Handling event replay and backfill
  - Step 5: CQRS pattern — separate write and read schemas
  - What's next (OUTBOX.md, PATTERNS.md §Event Sourcing)
**Priority:** P1 — HIGH
**Effort:** Large

### DOC-NEW-26: Create docs/tutorials/BACKFILL_AND_MIGRATION.md

**File:** `docs/tutorials/BACKFILL_AND_MIGRATION.md` (new)
**Description:** Tutorial — migrate a manually-maintained materialized
view to a stream table with zero downtime. Covers pre-migration analysis,
parallel running, cutover, and rollback.
**Structure:**
  - Goal and prerequisites
  - Step 1: Assess the existing matview (refresh frequency, query
    complexity, downstream dependencies)
  - Step 2: Validate query compatibility with `pgtrickle.validate_query()`
  - Step 3: Create the stream table alongside the existing matview
  - Step 4: Verify differential output matches matview during parallel run
  - Step 5: Redirect downstream consumers to the stream table
  - Step 6: Drop the old matview
  - Rollback procedure
  - What's next (MIGRATING_FROM_MATERIALIZED_VIEWS.md for the full
    migration guide)
**Priority:** P1 — HIGH
**Effort:** Medium

### DOC-NEW-27: Create docs/tutorials/SECURITY_HARDENING.md

**File:** `docs/tutorials/SECURITY_HARDENING.md` (new)
**Description:** Step-by-step guide to deploying pg_trickle with
minimum necessary privilege. Addresses schema ownership, role separation,
stream table definition injection risks, CDC trigger ownership, and
audit logging.
**Structure:**
  - Goal and prerequisites
  - Step 1: Create dedicated roles and schema ownership
  - Step 2: Grant minimum privileges for stream table management
  - Step 3: Restrict who can define stream tables (injection risk)
  - Step 4: Secure CDC trigger ownership
  - Step 5: Protect change buffers from direct reads
  - Step 6: Enable audit logging for stream table DDL
  - Step 7: Verify the hardened configuration
  - Security checklist (copy-paste verification queries)
  - What's next (SECURITY_MODEL.md, SECURITY_GUIDE.md)
**Priority:** P1 — HIGH
**Effort:** Medium

---

## 9. Phase 5 — Consistency Pass

To be executed in v0.57.0 after all content changes are complete.

### DOC-CONS-28: Terminology Consistency Sweep

- `stream table` (two words, lowercase) everywhere except document titles.
- `differential refresh` (not "diff refresh" or "incremental refresh").
- `change buffer` (two words, lowercase).
- `refresh frontier` (two words, lowercase).
- `CDC` (uppercase acronym), defined on first use per page.
- `DVM` (uppercase acronym), defined on first use per page.
- `DAG` (uppercase acronym), linked to GLOSSARY.md on first use.
**Fix:** Grep for common misspellings and variants; apply corrections
across all docs.

### DOC-CONS-29: Capitalization Sweep

- `pg_trickle` — always lowercase except in document titles.
- `PostgreSQL` — never `Postgres` in prose (only in user-provided SQL
  or quoted output).
- `pgtrickle` — schema name, always lowercase.
- `pgrx` — always lowercase.
**Fix:** Grep for `PgTrickle`, `Postgres` (non-quoted), and fix.

### DOC-CONS-30: Code Style Sweep

- All SQL keywords in UPPERCASE (`SELECT`, `FROM`, `WHERE`, `GROUP BY`).
- All function calls use the `pgtrickle.` schema prefix.
- All code blocks carry a language hint (` ```sql `, ` ```bash `,
  ` ```toml `).
**Fix:** Scan all code blocks; fix missing language hints and
non-prefixed function calls.

### DOC-CONS-31: Cross-Link Audit

- Verify every internal `[text](path)` link resolves to an existing file.
- Fix relative-path errors introduced by file moves or renames.
- Confirm introduction.md persona table links are all valid.
**Fix:** `grep -r '\[.*\](.*\.md' docs/` and validate each target.

---

## 10. Item Registry

| ID | Category | File | Priority | Effort | Release |
|----|----------|------|----------|--------|---------|
| DOC-P0-1 | Accuracy | `docs/GUC_CATALOG.md` | P0 | Medium | v0.56.0 |
| DOC-P0-2 | Completeness | `docs/ERRORS.md` | P0 | Large | v0.56.0 |
| DOC-P0-3 | Accuracy | `docs/CONFIGURATION.md` | P0 | Small | v0.56.0 |
| DOC-P1-4 | Completeness | `docs/SQL_REFERENCE.md` | P1 | Medium | v0.56.0 |
| DOC-P1-5 | Completeness | `docs/SQL_REFERENCE.md` | P1 | Medium | v0.56.0 |
| DOC-P1-6 | Structure | `docs/research/*.md` (×3) | P1 | Medium | v0.56.0 |
| DOC-P1-7 | Quality | `docs/DVM_REWRITE_RULES.md` | P1 | Medium | v0.56.0 |
| DOC-P1-8 | Structure | `docs/introduction.md` | P1 | Small | v0.56.0 |
| DOC-P1-9 | New content | `docs/MENTAL_MODEL.md` | P1 | Large | v0.56.0 |
| DOC-P1-10 | New content | `docs/LIMITATIONS.md` | P1 | Large | v0.56.0 |
| DOC-NEW-23 | New content | `docs/PERFORMANCE_CHEATSHEET.md` | P1 | Medium | v0.56.0 |
| DOC-P2-11 | Quality | `docs/SECURITY_GUIDE.md` | P2 | Small | v0.57.0 |
| DOC-P2-12 | Quality | `docs/WHATS_NEW.md` | P2 | Medium | v0.57.0 |
| DOC-P2-13 | Quality | `docs/tutorials/HYBRID_SEARCH_PATTERNS.md` | P2 | Medium | v0.57.0 |
| DOC-P2-14 | Quality | `docs/tutorials/PER_TENANT_ANN_PATTERNS.md` | P2 | Medium | v0.57.0 |
| DOC-P2-15 | Accuracy | `docs/QUICKSTART_5MIN.md`, `docs/GETTING_STARTED.md` | P2 | Small | v0.57.0 |
| DOC-P2-16 | Quality | `docs/PERFORMANCE_COOKBOOK.md` | P2 | Small | v0.57.0 |
| DOC-P2-17 | Completeness | `docs/SECURITY_MODEL.md` | P2 | Small | v0.57.0 |
| DOC-P3-18 | Polish | `docs/FAQ.md` | P3 | Small | v0.57.0 |
| DOC-P3-19 | Polish | `docs/DVM_OPERATORS.md` | P3 | Small | v0.57.0 |
| DOC-P3-20 | Polish | `docs/tutorials/VECTOR_RAG_STARTER.md` | P3 | Small | v0.57.0 |
| DOC-P3-21 | Polish | `docs/tutorials/tuning-refresh-mode.md` | P3 | Small | v0.57.0 |
| DOC-P3-22 | Polish | `docs/research/multi_db_refresh_broker.md` | P3 | Small | v0.57.0 |
| DOC-NEW-24 | New content | `docs/tutorials/FIRST_DASHBOARD.md` | P1 | Large | v0.57.0 |
| DOC-NEW-25 | New content | `docs/tutorials/EVENT_SOURCING.md` | P1 | Large | v0.57.0 |
| DOC-NEW-26 | New content | `docs/tutorials/BACKFILL_AND_MIGRATION.md` | P1 | Medium | v0.57.0 |
| DOC-NEW-27 | New content | `docs/tutorials/SECURITY_HARDENING.md` | P1 | Medium | v0.57.0 |
| DOC-CONS-28 | Consistency | All docs | P2 | Medium | v0.57.0 |
| DOC-CONS-29 | Consistency | All docs | P2 | Small | v0.57.0 |
| DOC-CONS-30 | Consistency | All docs | P2 | Small | v0.57.0 |
| DOC-CONS-31 | Consistency | All docs | P2 | Medium | v0.57.0 |

---

## 11. Implementation Sequencing

### v0.56.0 — Documentation Foundation

Theme: fix every inaccuracy; complete every reference; add the two
high-impact conceptual documents (`MENTAL_MODEL.md`, `LIMITATIONS.md`)
and the performance cheat sheet.

**Release objectives:**
1. GUC_CATALOG.md is accurate and machine-generated with no corruption.
2. ERRORS.md covers all 44 error variants in `src/error.rs`.
3. CONFIGURATION.md defaults match source code.
4. SQL_REFERENCE.md has examples for all 59 documented functions and
   column schemas for all 17 catalog tables.
5. Research stubs are self-contained with summaries.
6. DVM_REWRITE_RULES.md has worked SQL examples for all 5 passes.
7. introduction.md provides a conceptual on-ramp.
8. `docs/MENTAL_MODEL.md` exists and is complete.
9. `docs/LIMITATIONS.md` exists and covers the decision tree.
10. `docs/PERFORMANCE_CHEATSHEET.md` exists.

**Release gate:** `just lint` passes; all new content reviewed by at
least one maintainer; all SQL examples verified against a running
pg_trickle instance.

### v0.57.0 — Documentation Excellence

Theme: four new tutorials, P2/P3 polish, and a full consistency pass
across the entire corpus.

**Release objectives:**
1. All four new tutorials are complete, tested, and linked from
   introduction.md and SUMMARY.md.
2. SECURITY_GUIDE.md has copy-paste SQL templates.
3. WHATS_NEW.md has full summaries for all versions.
4. Tutorial gaps (HYBRID_SEARCH_PATTERNS, PER_TENANT_ANN_PATTERNS)
   resolved.
5. All P3 polish items applied.
6. Consistency pass complete: terminology, capitalisation, code style,
   cross-links verified across 83 files.

**Release gate:** Docs site builds cleanly; all internal links resolve;
terminology grep passes with zero mismatches.

---

## 12. Before/After Quality Assessment

| Dimension | Before (post Round 1) | After (post v0.57.0) |
|-----------|----------------------|---------------------|
| **Accuracy** | 2 critical errors (GUC_CATALOG, parallel_refresh_mode) | Zero known inaccuracies |
| **Completeness** | ERRORS.md 54%; 10 SQL functions without examples; 7 catalog tables undocumented; 3 new docs missing | ERRORS.md 100%; all functions with examples; all catalog tables documented; all 7 new docs created |
| **Structure & Navigation** | introduction.md undersized; 3 research stubs; DVM_REWRITE_RULES terse | introduction.md provides full on-ramp; all stubs have abstracts; all reference docs have examples |
| **Writing Quality** | Consistently professional; minor P3 gaps | Consistent throughout; P3 polish applied |
| **Tutorial Coverage** | 20 tutorials, 2 with pattern gaps; 4 tutorials missing | 24 tutorials; all complete and end-to-end tested |
| **Consistency** | Generally good; some link inconsistencies; some terminology drift | Verified across all 83 files |
