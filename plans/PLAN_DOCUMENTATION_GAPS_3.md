# Plan: Documentation Quality and Accuracy Gap Analysis - Round 3

**Date:** 2026-05-22
**Updated:** 2026-05-22 (all items implemented)
**Status:** IMPLEMENTED
**Scope:** `docs/`, generated documentation catalogs, documentation tooling, and
source-backed examples that describe current pg_trickle behavior.
**Predecessor:** [PLAN_DOCUMENTATION_GAPS_2.md](PLAN_DOCUMENTATION_GAPS_2.md)

---

## 1. Executive Summary

Round 3 focused on whether the documentation says things that are true for the
current implementation, not just whether the prose reads well. The most serious
stale areas were caused by fast-moving implementation changes since the previous
round:

1. The scheduler architecture is now launcher plus per-database schedulers, but
   parts of the docs still described a single scheduler worker.
2. Trigger-based CDC now defaults to statement-level triggers with transition
   tables, while many docs still described row-level triggers as the default.
3. Recursive CTE support exists in DIFFERENTIAL and IMMEDIATE modes, but one
   limitations page still called it unsupported.
4. The outbox, inbox, consumer-group, and relay APIs were extracted to pg_tide,
   but several pg_trickle docs still described old pg_trickle-owned APIs and
   GUCs.
5. The generated SQL API catalog missed `#[pgrx::pg_extern]` attributes, hiding
   `attach_embedding_outbox()` from the generated reference.

This pass fixed the most visible factual conflicts, but it also exposed a
larger documentation-system problem: the repo has generated catalogs, narrative
reference pages, architecture docs, FAQs, tutorials, and historical upgrade
notes that repeat the same source-of-truth facts without a drift guard. The
rest of this plan is an implementation roadmap to make those facts easier to
keep correct.

---

## 2. Audit Method

Commands and checks used during this round:

- `python3 check_links.py` - local docs link check; passed with zero broken local
  links before and after the initial audit.
- `python3 scripts/gen_catalogs.py --check` - generated catalog drift check;
  initially passed, then revealed a generator blind spot after manual source
  inspection.
- Source cross-checks against:
  - `src/config.rs` for GUC names, defaults, and trigger-mode defaults.
  - `src/scheduler/mod.rs` and `src/scheduler/scheduler_loop.rs` for launcher
    and per-database scheduler behavior.
  - `src/api/outbox.rs` for the current pg_tide integration functions.
  - `src/dvm/parser/validation.rs` and `src/dvm/diff.rs` for recursive CTE and
    IMMEDIATE-mode support.
  - `docs/GUC_CATALOG.md` and `docs/SQL_API_CATALOG.md` for generated surfaces.
- Targeted stale-term searches for removed or renamed APIs such as
  `enable_outbox`, `create_inbox`, `poll_outbox`, `pg_trickle.outbox_*`,
  `parallel_refresh_mode = 'off'`, and row-trigger default claims.

---

## 3. Corrections Already Made in This Pass

These are complete and should not be duplicated by follow-up work:

| Area | Files | Change |
|------|-------|--------|
| Scheduler architecture | `docs/ARCHITECTURE.md`, `docs/FAQ.md`, `docs/GETTING_STARTED.md` | Updated wording from a single scheduler worker to one launcher plus per-database schedulers; corrected parallel refresh default to `on`. |
| Recursive CTE support | `docs/LIMITATIONS.md`, `docs/GETTING_STARTED.md` | Removed the stale unsupported claim and documented bounded semi-naive / DRed support in IMMEDIATE mode. |
| pg_tide extraction | `docs/SQL_REFERENCE.md`, `docs/OUTBOX.md`, `docs/INBOX.md`, `docs/PATTERNS.md`, `docs/CONFIGURATION.md`, `docs/ARCHITECTURE.md`, `docs/UPGRADING.md` | Replaced old pg_trickle outbox/inbox/consumer API docs with the current `attach_outbox`, `detach_outbox`, `attach_embedding_outbox`, and manual inbox guidance. |
| GUC truthfulness | `docs/CONFIGURATION.md`, `docs/ARCHITECTURE.md` | Corrected `invalidation_ring_capacity` default/range, `min_schedule_seconds`, `block_source_ddl`, and removed non-existent outbox/inbox GUC sections. |
| CDC trigger default | `docs/CDC_MODES.md`, `docs/CONFIGURATION.md`, `docs/ARCHITECTURE.md`, `docs/FAQ.md`, `docs/GETTING_STARTED.md`, `docs/introduction.md`, `docs/GLOSSARY.md`, `docs/MENTAL_MODEL.md`, `docs/DEMO.md`, `docs/SECURITY_GUIDE.md`, `docs/tutorials/WHAT_HAPPENS_ON_TRUNCATE.md` | Updated high-level wording to statement-level trigger default with row-level trigger compatibility. |
| Generated catalog tooling | `scripts/gen_catalogs.py`, `docs/SQL_API_CATALOG.md` | Added support for `#[pgrx::pg_extern]` and regenerated the SQL catalog, increasing function count from 122 to 123. |

---

## 4. Remaining High-Priority Findings

### DOC3-P0-01: Add a Source-Surface Drift Gate

**Problem:** The docs repeat SQL function names, GUC names, defaults, and
catalog table names by hand. The current link checker cannot detect whether a
GUC or SQL function mentioned in prose exists.

**Implementation:**

1. Add `scripts/check_docs_truth.py`.
2. Extract source truth from `src/config.rs`, generated `docs/GUC_CATALOG.md`,
   and `docs/SQL_API_CATALOG.md`.
3. Scan Markdown files for:
   - `pg_trickle.<name>` GUC references not in the generated GUC catalog.
   - `pgtrickle.<function>(` examples not in the generated SQL API catalog.
   - Removed pg_trickle-owned pg_tide APIs (`enable_outbox`, `poll_outbox`,
     `create_inbox`, etc.) outside historical notes.
   - Stale default phrases such as `parallel_refresh_mode = 'off'` as default.
4. Support an allowlist for historical upgrade sections and explicit negative
   statements such as "pg_trickle does not expose `poll_outbox`."
5. Wire it into CI next to `scripts/gen_catalogs.py --check`.

**Acceptance criteria:**

- CI fails when prose references a non-existent current GUC or SQL function.
- Historical mentions must include an allowlist reason.
- The script prints file, line, token, and suggested owner document.

### DOC3-P0-02: Make `CONFIGURATION.md` Match the 129-GUC Reality

**Problem:** `CONFIGURATION.md` no longer claims to be exhaustive, but it still
looks like a complete reference because it has a long table of contents and many
detailed sections. The generated catalog has 129 GUCs; the narrative guide only
covers a subset.

**Implementation:**

1. Decide and document the split:
   - `GUC_CATALOG.md` is exhaustive and generated.
   - `CONFIGURATION.md` is curated and task-oriented.
2. Add a short "Coverage policy" section near the top of `CONFIGURATION.md`.
3. Add a table mapping every GUC category from `GUC_CATALOG.md` to either:
   - detailed section in `CONFIGURATION.md`,
   - generated-only entry,
   - internal/experimental entry with a short warning.
4. Add missing high-value sections for current operational GUCs:
   - `pg_trickle.change_buffer_durability`
   - `pg_trickle.cdc_paused`
   - `pg_trickle.force_full_refresh`
   - `pg_trickle.wal_max_changes_per_poll`
   - `pg_trickle.wal_max_lag_bytes`
   - `pg_trickle.enable_fused_refresh`
   - DuckLake sink reliability/security GUCs.

**Acceptance criteria:**

- A reader can tell immediately whether `CONFIGURATION.md` is curated or
  exhaustive.
- Every GUC in `GUC_CATALOG.md` has an owner category and at least one sentence
  of human guidance somewhere in docs or is explicitly marked internal.

### DOC3-P0-03: Finish the CDC Trigger-Mode Rewrite Across Tutorials

**Problem:** The top-level docs now say statement-level triggers are the default,
but several deep tutorials still use row-level-trigger mental models in tables
and diagrams. Some of those are valid when comparing scheduled DIFFERENTIAL mode
with IMMEDIATE mode, but the wording no longer matches the default.

**Implementation:**

1. Audit these files line by line:
   - `docs/tutorials/WHAT_HAPPENS_ON_INSERT.md`
   - `docs/tutorials/WHAT_HAPPENS_ON_UPDATE.md`
   - `docs/tutorials/WHAT_HAPPENS_ON_DELETE.md`
   - `docs/tutorials/WHAT_HAPPENS_ON_TRUNCATE.md`
   - `docs/tutorials/PARTITIONED_TABLES.md`
   - `docs/tutorials/FOREIGN_TABLE_SOURCES.md`
   - `docs/SQL_REFERENCE.md` sections on foreign sources and logical replication.
2. Replace default-path descriptions with:
   - scheduled trigger CDC: statement-level trigger with transition tables,
   - legacy compatibility: row-level trigger mode,
   - IMMEDIATE mode: statement-level IVM triggers with transition tables and no
     change-buffer scheduling.
3. Keep explicit comparisons to row-level mode only where they are marked as
   legacy/diagnostic or conceptual.

**Acceptance criteria:**

- No tutorial says row-level triggers are the default.
- TRUNCATE docs consistently say pg_trickle captures a statement-level marker
  and falls back to full refresh for affected stream tables.
- The distinction between scheduled statement-level CDC triggers and IMMEDIATE
  statement-level IVM triggers is clear.

### DOC3-P0-04: Add a Stream-Table Status Lifecycle Reference

**Problem:** Docs mention `INITIALIZING`, `ACTIVE`, `SUSPENDED`, and `ERROR`,
but there is no authoritative lifecycle explanation.

**Implementation:**

1. Add a `Stream table lifecycle` section to `docs/SQL_REFERENCE.md` near the
   catalog/status functions.
2. Include:
   - status meanings,
   - transitions at create, first refresh, manual suspend/resume, auto-suspend,
     DDL reinit, and repair,
   - how `consecutive_errors`, `needs_reinit`, and `is_populated` interact,
   - which functions are allowed in each status.
3. Cross-link from `docs/TROUBLESHOOTING.md`, `docs/FAQ.md`, and
   `docs/ARCHITECTURE.md`.

**Acceptance criteria:**

- Every status value in `pgt_stream_tables.status` has a definition and recovery
  action.
- Troubleshooting pages link to the lifecycle reference instead of repeating
  partial explanations.

---

## 5. Medium-Priority Accuracy and Coverage Work

### DOC3-P1-01: Complete SQL Reference Coverage from `SQL_API_CATALOG.md`

**Problem:** `SQL_API_CATALOG.md` lists 123 SQL-callable functions. The main
SQL reference does not give dedicated user guidance for several current
functions, especially diagnostics, scheduler controls, self-monitoring, gate
APIs, and generated helper APIs.

**Implementation:**

1. Generate a coverage report: function in catalog, detailed section in
   `SQL_REFERENCE.md`, short mention only, or intentionally internal.
2. Add a compact "Advanced and diagnostic functions" table for functions that
   should not get full sections.
3. Add detailed sections for high-value missing functions:
   - `cdc_pause_status`
   - `drain` / `is_drained`
   - `preflight`
   - `cluster_worker_summary`
   - `validate_query`
   - `stream_table_lineage`
   - `recommend_schedule` / `schedule_recommendations`
   - `setup_self_monitoring` / `self_monitoring_status`
   - `attach_embedding_outbox` if it remains public.
4. Mark internal or migration-only functions explicitly.

**Acceptance criteria:**

- Every SQL-callable function has one of: full docs, table entry, or explicit
  internal/experimental classification.
- SQL examples use parameter names from source.

### DOC3-P1-02: Create a Documentation Source-of-Truth Map

**Problem:** The same facts appear in many places. For example, CDC architecture
is described in `ARCHITECTURE.md`, `CDC_MODES.md`, `FAQ.md`, `GETTING_STARTED.md`,
`MENTAL_MODEL.md`, tutorials, and benchmark commentary.

**Implementation:**

Add `docs/DOCS_OWNERSHIP.md` or a section in `docs/SUMMARY.md` that defines:

| Fact Type | Source of Truth | Other Docs Should |
|-----------|-----------------|-------------------|
| GUC names/defaults | `GUC_CATALOG.md` | link or summarize only |
| SQL signatures | `SQL_API_CATALOG.md` + `SQL_REFERENCE.md` | link to SQL reference |
| Architecture | `ARCHITECTURE.md` | use conceptual summaries |
| CDC behavior | `CDC_MODES.md` | link for details |
| Support limits | `LIMITATIONS.md` | avoid local unsupported lists |
| Operational recovery | `TROUBLESHOOTING.md` / runbooks | link from FAQ |

**Acceptance criteria:**

- New docs have an obvious place to link instead of copying canonical facts.
- Reviewers can reject duplicate source-of-truth prose during PR review.

### DOC3-P1-03: Add Feature Readiness Labels

**Problem:** Some docs read as production-ready while the actual maturity varies
by feature. Citus, storage backends, DuckLake sinks, pg_tide integration,
self-monitoring, and advanced vector features need consistent labels.

**Implementation:**

1. Define labels: `Stable`, `Beta`, `Experimental`, `Design only`, `Moved to
   pg_tide`, `Internal`.
2. Add labels to:
   - `docs/CITUS.md` and `docs/integrations/citus.md`
   - `docs/STORAGE_BACKENDS.md`
   - `docs/tutorial-pg-tide-ducklake-pipeline.md`
   - `docs/OUTBOX.md` and `docs/INBOX.md`
   - vector and DuckLake tutorials.
3. Back each label with tests or source evidence.

**Acceptance criteria:**

- No integration guide leaves readers guessing whether a feature is GA,
  experimental, or external.
- Labels are defined once and reused consistently.

### DOC3-P1-04: Improve Generated Catalog Quality

**Problem:** Generated catalog descriptions are useful but sometimes awkward:
multiline doc comments collapse into long one-line cells, return types such as
`TableIterator<` are truncated, and SQL names with `name = "..."` need better
normalization.

**Implementation:**

1. Improve return-type parsing for multiline generic returns.
2. Respect `name = "..."` in the displayed SQL function name.
3. Add an "Internal/generated" column for functions with `sql = false` or known
   infrastructure-only behavior.
4. Add `--check` tests for qualified `#[pgrx::pg_extern]` attributes.

**Acceptance criteria:**

- `docs/SQL_API_CATALOG.md` is readable enough to be a real quick reference.
- Catalog generation has unit-test-like fixtures for the attribute patterns used
  in this repo.

---

## 6. Structure, Duplication, and Reader Experience

### DOC3-P2-01: Split and De-Duplicate the FAQ

**Problem:** `docs/FAQ.md` is very large and repeats architecture, CDC, tuning,
and troubleshooting details that belong in canonical docs.

**Implementation:**

1. Keep short answers in FAQ.
2. Move long operational recipes to `TROUBLESHOOTING.md`, `CDC_MODES.md`, or
   relevant runbooks.
3. Add links to canonical sections instead of repeated prose.
4. Preserve anchors for existing inbound links where possible.

**Acceptance criteria:**

- FAQ answers are short enough to scan.
- No FAQ section owns a source-of-truth fact already owned by a reference doc.

### DOC3-P2-02: Standardize Examples

**Problem:** Examples mix `schedule_seconds`, `schedule => '5s'`, positional
arguments, named arguments, schema qualification, and old API names.

**Implementation:**

1. Prefer named arguments for public SQL examples.
2. Prefer `schedule => '5s'` unless the source API specifically uses another
   parameter.
3. Use schema-qualified stream table names in examples.
4. Add an example linter that flags removed function names and deprecated
   parameters.

**Acceptance criteria:**

- Examples across Getting Started, SQL Reference, Patterns, and tutorials use a
  consistent style.
- Removed API names only appear in historical notes or migration sections.

### DOC3-P2-03: Add Docs Navigation for Reader Roles

**Problem:** The docs are broad enough that users need paths by role.

**Implementation:**

Add a concise navigation table to `docs/SUMMARY.md` or `docs/introduction.md`:

| Role | Start Here | Next |
|------|------------|------|
| Application developer | Getting Started | SQL Reference, Patterns |
| DBA/SRE | Installation, Configuration | Pre-deployment, Troubleshooting |
| Data engineer | Mental Model | Performance Cookbook, DVM Operators |
| Evaluator | Essence, Comparisons | Limitations, Benchmark |
| Extension contributor | Architecture | Plans, Testing docs |

**Acceptance criteria:**

- A new reader can choose a path in under one minute.
- Deep reference docs are not the first stop for casual evaluation.

---

## 7. Automation and Validation Checklist

Every documentation improvement PR should run:

```bash
python3 check_links.py
python3 scripts/gen_catalogs.py --check
python3 scripts/check_docs_truth.py   # new in DOC3-P0-01
just fmt
just lint
```

For SQL examples that create stream tables or call public functions, add a
future `scripts/check_sql_examples.py` that extracts fenced `sql` blocks marked
with `-- doc-test` and runs them against the light E2E container.

---

## 8. Proposed Sequencing

| Phase | Items | Target |
|-------|-------|--------|
| Phase 1 - Truth guardrails | DOC3-P0-01, DOC3-P1-04 | Next docs PR |
| Phase 2 - Reference correctness | DOC3-P0-02, DOC3-P0-03, DOC3-P0-04, DOC3-P1-01 | Same milestone as next SQL/API changes |
| Phase 3 - Reader structure | DOC3-P1-02, DOC3-P2-01, DOC3-P2-02 | After reference correctness |
| Phase 4 - Product clarity | DOC3-P1-03, DOC3-P2-03 | Before next public release |

---

## 9. Definition of Done

Round 3 should be considered complete when:

1. Link checks, generated catalog checks, and docs truth checks all pass in CI.
2. `CONFIGURATION.md` and `SQL_REFERENCE.md` clearly state what is exhaustive
   versus curated.
3. No current docs describe removed pg_trickle-owned outbox/inbox APIs as active.
4. No current docs describe row-level CDC triggers as the default.
5. No current docs describe recursive CTEs as unsupported.
6. Stream-table lifecycle statuses are documented in one canonical place.
7. Feature-readiness labels exist for integrations and advanced features.
8. The FAQ and tutorials link to source-of-truth reference docs rather than
   copying long, drift-prone explanations.
