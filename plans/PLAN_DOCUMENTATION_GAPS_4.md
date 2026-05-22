# Documentation Improvement Proposal
Generated: 2026-05-22
Scope: All docs/

## Summary

20 issues found across 7 review phases: 3 critical (function signature boxes are
materially incomplete), 3 major (missing error types, rewrite passes, GUC count
discrepancy), 2 minor (broken link, terminology drift), and 4 gaps plus 3
structural recommendations. The onboarding path (quickstart + getting started)
is well-structured and accurate — no changes required there.

```
Documentation Review Progress:
- [x] Phase 1: Inventory
- [x] Phase 2: Accuracy check
- [x] Phase 3: Link check
- [x] Phase 4: Gap analysis
- [x] Phase 5: Consistency & duplication
- [x] Phase 6: Readability
- [x] Phase 6.5: Onboarding path audit
- [x] Phase 7: Proposal
```

---

## Critical (breaks user experience)

### C1 — `create_stream_table()` signature box missing 9 parameters
- **File:** `docs/SQL_REFERENCE.md`, line 145
- **Issue:** The signature box shows 10 parameters. The actual function
  (`src/api/create.rs`, line 26) has 19. Missing parameters:
  `partition_by`, `max_differential_joins`, `max_delta_fraction`,
  `output_distribution_column`, `temporal`, `storage_backend`, `sink`,
  `ducklake_sink_path`, `ducklake_sink_table_id`.
- **Impact:** Users who read the signature box to discover available arguments
  are unaware these parameters exist — including the entire DuckLake sink
  integration and columnar/temporal modes.
- **Fix:** Expand the signature block and the parameter table. Note that
  `partition_by`, `fuse`, etc. are documented elsewhere in the same file (dbt
  integration section, `partition_by config` section) but are absent from the
  canonical signature section that users reach first.

### C2 — `create_or_replace_stream_table()` signature box missing 6 parameters
- **File:** `docs/SQL_REFERENCE.md`, line 766
- **Issue:** Signature shows 10 params. Actual (`src/api/create.rs`, line 330)
  has 16. Missing: `partition_by`, `max_differential_joins`,
  `max_delta_fraction`, `output_distribution_column`, `temporal`,
  `storage_backend`.
- **Fix:** Expand the signature box and parameter table to match the code.

### C3 — `alter_stream_table()` signature box missing 11 parameters
- **File:** `docs/SQL_REFERENCE.md`, line 872
- **Issue:** Signature shows 11 parameters. Actual (`src/api/alter.rs`,
  line 1630) has 22. Missing: `fuse`, `fuse_ceiling`, `fuse_sensitivity`,
  `partition_by`, `max_differential_joins`, `max_delta_fraction`,
  `post_refresh_action`, `reindex_drift_threshold`, `sink`,
  `ducklake_sink_path`, `ducklake_sink_table_id`.
- **Note:** `fuse`, `fuse_ceiling`, `fuse_sensitivity`, and `partition_by` are
  documented in the dbt integration section lower in the same file but are
  absent from the function signature box.
- **Fix:** Expand the signature box and add missing parameters to the parameter
  table.

---

## Major (misleads or confuses)

### M1 — ERRORS.md missing entire DuckLake error section (6 variants)
- **File:** `docs/ERRORS.md` — no DuckLake section exists
- **Issue:** Six error variants introduced with the DuckLake sink feature are
  completely absent. Source: `src/error.rs`, lines 177–198.
  - `DuckLakeSnapshotExpired`
  - `DuckLakeChangeFeedError`
  - `DucklakeParquetError`
  - `DucklakeUploadError`
  - `DucklakeCatalogError`
  - `DucklakeSinkError`
- **Fix:** Add a `## DuckLake / Sink Errors` section covering all six variants
  with description, common causes, and remediation steps.

### M2 — DVM_REWRITE_RULES.md missing 7 rewrite passes
- **File:** `docs/DVM_REWRITE_RULES.md` — currently covers only 5 passes
- **Issue:** The following public rewrite functions in
  `src/dvm/parser/rewrites.rs` have no documentation:

  | Function | Line |
  |---|---|
  | `rewrite_distinct_on` | 995 |
  | `rewrite_correlated_scalar_in_select` | 1956 |
  | `rewrite_demorgan_sublinks` | 2973 |
  | `rewrite_sublinks_in_or` | 3243 |
  | `rewrite_rows_from` | 3650 |
  | `rewrite_multi_partition_windows` | 4092 |
  | `rewrite_nested_window_exprs` | 4459 |

- **Fix:** Add a section for each missing pass describing the triggering SQL
  pattern and what the rewrite does to enable differential maintenance.

### M3 — GUC_CATALOG.md missing `pg_trickle.self_monitoring_auto_apply` + wrong count
- **File:** `docs/GUC_CATALOG.md` — header claims "**129** configuration parameters"
- **Issue:** The GUC `pg_trickle.self_monitoring_auto_apply`
  (`src/config.rs`, line 2528) is absent from the auto-generated catalog.
  Root cause: the static declaration `PGS_SELF_MONITORING_AUTO_APPLY`
  (`src/config.rs`, line 903) has no `///` doc comment, so `gen_catalogs.py`
  silently skips it. The true count is 130. The GUC is documented in
  `docs/CONFIGURATION.md` (line 2179) but absent from the catalog.
- **Fix:**
  1. Add a `///` doc comment to `PGS_SELF_MONITORING_AUTO_APPLY` in
     `src/config.rs` at line 903.
  2. Re-run `python3 scripts/gen_catalogs.py` to update the catalog.

---

## Minor (polish, consistency)

### m1 — Broken link in SUMMARY.md (`blog/README.md`)
- **File:** `docs/SUMMARY.md`, line 143
- **Issue:** The link target `blog/README.md` resolves relative to `docs/` as
  `docs/blog/README.md`, which does not exist. The blog directory is at the
  repository root (`blog/README.md`).
- **Fix:** Change to `../blog/README.md`, or if the mdBook build does not allow
  links outside `docs/`, replace with the live URL or remove the entry.

### m2 — "incremental refresh" used instead of authoritative "differential refresh"
- **Files:**
  - `docs/FAQ.md`, lines 170, 190, 937
  - `docs/BENCHMARK.md`, lines 83, 184
  - `docs/WHATS_NEW.md`, line 107
  - `docs/tutorials/MIGRATING_FROM_MATERIALIZED_VIEWS.md`, line 28
  - `docs/tutorials/MIGRATING_FROM_PG_IVM.md`, line 46
  - `docs/integrations/dbt.md`, line 6
- **Issue:** "incremental refresh" is used loosely across these files.
  `docs/GLOSSARY.md` defines the authoritative term as "differential refresh"
  (maps to `refresh_mode = 'DIFFERENTIAL'`). "IVM" is the academic umbrella
  term; the extension's branded, user-facing term is "differential".
- **Fix:** Replace "incremental refresh" with "differential refresh" in the
  listed locations, or add a GLOSSARY cross-reference note clarifying the
  equivalence.

---

## Gaps (undocumented features)

### G1 — DuckLake error variants absent from ERRORS.md
- **Missing from:** `docs/ERRORS.md`
- **Source:** `src/error.rs`, lines 177–198
- **Variants:** `DuckLakeSnapshotExpired`, `DuckLakeChangeFeedError`,
  `DucklakeParquetError`, `DucklakeUploadError`, `DucklakeCatalogError`,
  `DucklakeSinkError`
- **Action:** See M1 above.

### G2 — 7 rewrite passes absent from DVM_REWRITE_RULES.md
- **Missing from:** `docs/DVM_REWRITE_RULES.md`
- **Source:** `src/dvm/parser/rewrites.rs`
- **Passes:** `rewrite_distinct_on`, `rewrite_correlated_scalar_in_select`,
  `rewrite_demorgan_sublinks`, `rewrite_sublinks_in_or`, `rewrite_rows_from`,
  `rewrite_multi_partition_windows`, `rewrite_nested_window_exprs`
- **Action:** See M2 above.

### G3 — `pg_trickle.self_monitoring_auto_apply` absent from GUC_CATALOG.md
- **Missing from:** `docs/GUC_CATALOG.md`
- **Source:** `src/config.rs`, line 2528
- **Root cause:** No `///` doc comment on `PGS_SELF_MONITORING_AUTO_APPLY`
  (`src/config.rs`, line 903) → gen script skips it silently.
- **Action:** See M3 above.

### G4 — `create_or_replace_stream_table` behaviour table missing newer params
- **Missing from:** `docs/SQL_REFERENCE.md`, line 788 (the "Behavior" table)
- **Issue:** The "Alter config" row in the behaviour table names only the
  original 6 parameters (schedule, refresh_mode, diamond settings, cdc_mode,
  append_only, pooler_compatibility_mode). The 6 newer parameters added since
  v0.32 are silently passed through without any mention.
- **Fix:** Update the behaviour table to list all parameters subject to
  alter-on-replace logic, or add a note that all parameters accepted by
  `create_or_replace_stream_table` are subject to alter-on-config-change.

---

## Recommendations (structural)

### R1 — Auto-generate SQL_REFERENCE.md signature blocks from source
- **Rationale:** `GUC_CATALOG.md` and `SQL_API_CATALOG.md` use
  `gen_catalogs.py` with CI drift checks and stay accurate. The
  `SQL_REFERENCE.md` signature boxes are hand-written and now lag by 9–11
  parameters across 3 core functions. Every parameter addition will silently
  diverge unless a human notices and updates the doc.
- **Action:** Extend `gen_catalogs.py` to also output the parameter list for
  each core function signature, or add a CI step that extracts the actual
  function signatures from `src/api/` and compares parameter counts against the
  signature boxes in `SQL_REFERENCE.md`.

### R2 — Require `///` doc comments on all `pub static PGS_*: GucSetting<...>`
- **Rationale:** `PGS_SELF_MONITORING_AUTO_APPLY` has no doc comment, causing
  the gen script to silently omit it from GUC_CATALOG.md. This will recur for
  future GUC statics added without `///` comments.
- **Action:** Add the following item to the Code Review Checklist in
  `AGENTS.md`:
  > Every `pub static PGS_*: GucSetting<...>` must have at least one `///`
  > doc comment immediately before it, or `gen_catalogs.py` will silently omit
  > it from `docs/GUC_CATALOG.md`.

### R3 — Onboarding path is accurate — no changes required
- `docs/QUICKSTART_5MIN.md` and `docs/GETTING_STARTED.md` are accurate: all
  referenced functions exist in code (`create_stream_table`,
  `refresh_stream_table`, `drop_stream_table`, `pgt_status`, `health_check`,
  `explain_st`), prerequisites are listed upfront (PostgreSQL 18,
  `shared_preload_libraries`, `max_worker_processes`), expected outputs are
  shown after every SQL block, and next-step navigation is clear.
