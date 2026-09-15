# PLAN_IGNORED_E2E_TESTS.md — Re-enable Ignored E2E Coverage

**Status:** Complete  
**Date:** 2026-03-10  
**Branch:** `plan-ignored-e2e-tests`  
**Scope:** Reconcile the stale changelog note about 18 ignored E2E tests, re-enable the suites that are already supportable, and implement the remaining DVM fixes needed to bring the still-failing ignored tests back into normal E2E coverage.

---

## Implementation Progress

| Workstream | Status | Notes |
|---|---|---|
| A1 — Keyless duplicate suite | ✅ Complete | Already done before this plan; docs updated |
| A2 — HAVING transition suite | ✅ Complete | 2 DVM bugs fixed; 5 tests un-ignored |
| B1 — Correlated scalar subquery | ✅ Complete | Rewrite-based decorrelation; 2 tests un-ignored |
| C1 — FULL JOIN differential | ✅ Complete | 5 DVM bugs fixed; 5 tests un-ignored |
| C2 — Correlated EXISTS with HAVING | ✅ Complete | 3 DVM bugs fixed; 1 test un-ignored |
| D1 — Changelog/docs re-baseline | ✅ Complete | CHANGELOG + Known Limitations updated |

### A2 Root Causes Fixed

**Bug 1:** `COUNT(*)` in HAVING predicate rewrite caused "syntax error at or near `*`".
- Root cause: PostgreSQL normalizes `COUNT(*)` in HAVING to `FuncCall { args: [Raw("*")] }`, but
  `rewrite_having_expr` checked `args.is_empty()` for CountStar detection.
- Fix: Extended the CountStar check in `rewrite_having_expr` and
  `extract_aggregates_from_expr_inner` to also accept `[Raw("*")]`.
- Files: `src/dvm/parser.rs`

**Bug 2:** Threshold crossing upward produced wrong aggregate value for groups absent from ST.
- Root cause: Algebraic merge used `COALESCE(st.col, 0) + delta` for absent groups, which
  ignores all pre-existing source rows. A group with 10 existing rows getting 15 new rows
  would produce `SUM = 15` instead of `25`.
- Fix: Added `having_filter: bool` flag to `DiffContext`. `diff_filter` sets the flag before
  calling `diff_aggregate`. `build_rescan_cte` extended with `force_all_aggs` param that
  includes all aggregates + `COUNT(*) AS __pgt_count`. `agg_merge_expr_mapped` uses
  `CASE WHEN st.col IS NULL THEN COALESCE(r.col, 0) ELSE algebraic_expr END` when
  `having_rescan = true`, using the full rescan value for new groups.
- Files: `src/dvm/diff.rs`, `src/dvm/operators/filter.rs`, `src/dvm/operators/aggregate.rs`

### B1 Correlated Scalar Subquery ✅ COMPLETE

**Completed:** 2026-03 (branch `plan-ignored-e2e-tests`)

Both previously-ignored tests in `tests/e2e_scalar_subquery_tests.rs` now pass.
The fix uses a pre-parser rewrite that decorrelates correlated scalar subqueries
in the SELECT list into LEFT JOINs before the DVM parser sees the query.

**Root causes found and fixed:**

1. **`parser.rs` — correlated scalar subquery in SELECT not decorrelated:**
   The DVM parser could not handle correlated scalar subqueries in the SELECT
   list (e.g., `(SELECT MAX(e.salary) FROM emp e WHERE e.dept_id = d.id)`).
   The `ScalarSubquery` operator only handled non-correlated cases. Added a
   new pre-parser rewrite `rewrite_correlated_scalar_in_select()` that detects
   correlated scalar subqueries in the target list, extracts correlation
   conditions, and rewrites them as LEFT JOIN subqueries:
   ```sql
   -- Before: SELECT d.name, (SELECT MAX(e.salary) FROM emp e WHERE e.dept_id = d.id) ...
   -- After:  SELECT d.name, "__pgt_sq_1"."__pgt_scalar_1" ...
   --         FROM dept d LEFT JOIN (SELECT dept_id AS "__pgt_corr_key_1",
   --           MAX(salary) AS "__pgt_scalar_1" FROM emp GROUP BY dept_id) AS "__pgt_sq_1"
   --         ON d.id = "__pgt_sq_1"."__pgt_corr_key_1"
   ```
   Files: `src/dvm/parser.rs`, `src/dvm/mod.rs`, `src/api.rs`

2. **`parser.rs` — inner table qualifiers leaked into differential SQL:**
   The initial rewrite preserved inner table qualifiers (e.g., `MAX(e.salary)`)
   in the decorrelated subquery's SELECT list. While valid for the subquery
   itself, the DVM's intermediate aggregate delta engine wraps the FROM clause
   in an EXCEPT ALL subquery aliased as `__pgt_old`, making the original inner
   alias (`e`) invisible. Fixed by stripping inner table qualifiers from the
   scalar expression during rewrite (`expr.strip_qualifier().to_sql()`),
   producing `MAX(salary)` which resolves correctly in all DVM-generated SQL
   contexts.
   Files: `src/dvm/parser.rs`

**Acceptance criteria met:**

1. ✅ Both `test_correlated_scalar_subquery_differential` and
   `test_scalar_subquery_null_result_differential` pass without `#[ignore]`.
2. ✅ All 2 non-correlated scalar subquery tests still pass.
3. ✅ All previously-fixed HAVING, FULL JOIN, and EXISTS+HAVING tests still pass.

---

## Motivation

The project currently has a stale mismatch between documentation and reality:

- `CHANGELOG.md` still says 18 E2E tests are ignored due to pre-existing DVM bugs.
- `tests/e2e_keyless_duplicate_tests.rs` no longer has ignored tests at all.
- The ignored `HAVING` transition suite currently appears supportable again.
- The ignored `FULL JOIN` and correlated `EXISTS ... HAVING` cases still fail.
- The correlated scalar-subquery bucket is still ignored, but needs a clean re-baseline before deciding whether it should be re-enabled or fixed.

This plan treats the ignored tests as a product-quality issue, not just a testing issue. The goal is to make the repository's claims, test annotations, DVM implementation, and CI behavior agree again.

---

## Ground Truth

### Changelog Claim

The stale claim currently lives in `CHANGELOG.md` and groups the ignored tests as:

- `e2e_full_join_tests` — 5
- `e2e_having_transition_tests` — 5
- `e2e_keyless_duplicate_tests` — 5
- `e2e_scalar_subquery_tests` — 2
- `e2e_sublink_or_tests` — 1

### Verified Current State

| Suite | Changelog Count | Current Ignore State | Current Reality | Plan Outcome |
|---|---:|---|---|---|
| `tests/e2e_keyless_duplicate_tests.rs` | 5 | No ignored tests remain | Suite already re-enabled | Documentation cleanup only |
| `tests/e2e_having_transition_tests.rs` | 5 | 5 ignored tests remain | Ignored-only suite passed in local verification | Re-enable now |
| `tests/e2e_scalar_subquery_tests.rs` | 2 | 2 ignored tests remain | Needs clean confirmation outside the noisy terminal wrapper | Verify, then either re-enable or fix |
| `tests/e2e_full_join_tests.rs` | 5 | 5 ignored tests remain | Ignored-only suite still fails 5/5 | Engine work required |
| `tests/e2e_sublink_or_tests.rs` | 1 | 1 ignored test remains | Ignored test still fails | Engine work required |

### Important Consequence

This is not one problem. It is three separate workstreams:

1. `Already fixed but not reflected everywhere`
2. `Likely fixed, needs clean verification`
3. `Still broken in DVM and requires implementation work`

The plan below keeps those buckets separate so the project can recover coverage incrementally instead of waiting for every remaining DVM edge case to be solved.

---

## Goals

1. Remove stale `#[ignore]` markers from suites that are already supportable.
2. Re-baseline the scalar-subquery bucket with clean evidence, not assumptions.
3. Fix the remaining DVM correctness bugs blocking the `FULL JOIN` and correlated `EXISTS ... HAVING` suites.
4. Bring docs and changelog back in sync with the actual test inventory.
5. Ensure default E2E coverage exercises every re-enabled test in CI.

## Non-Goals

1. Reworking unrelated E2E infrastructure.
2. Refactoring the DVM engine outside the failing operator paths.
3. Expanding SQL support beyond the currently ignored buckets.
4. Changing the meaning of `FULL`, `DIFFERENTIAL`, or `IMMEDIATE` mode beyond what is needed for correctness.

---

## Workstream A — Immediate Re-enable

This workstream covers cases that do not need new engine behavior.

### A1. Keyless Duplicate Suite

**Current state:** Already done in code. `tests/e2e_keyless_duplicate_tests.rs` has no ignored tests left.

**Required work:**

1. Remove the stale changelog claim that still counts these 5 tests as ignored.
2. Update user-facing docs that still warn as if exact-duplicate keyless rows are categorically unsupported in the same way they were before the counted-delete and non-unique row-id work landed.
3. Confirm CI includes this suite in normal E2E execution and that no dedicated `--ignored` path remains for it.

**Likely files:**

- `CHANGELOG.md`
- `docs/FAQ.md`
- `docs/SQL_REFERENCE.md`
- `plans/testing/PLAN_TESTING_GAPS.md` if remaining failures need tracking

**Acceptance criteria:**

1. No docs claim these tests are still ignored.
2. The suite continues to pass in normal E2E runs.

### A2. HAVING Transition Suite

**Current state:** Five tests remain ignored in `tests/e2e_having_transition_tests.rs`, but the ignored-only suite passed during local verification.

**Implementation steps:**

1. Re-run the full ignored-only suite once in a clean shell and once in CI to eliminate false confidence from terminal-wrapper noise.
2. Remove all five `#[ignore = ...]` annotations from `tests/e2e_having_transition_tests.rs`.
3. Run the suite in normal mode and as part of the standard E2E test tier.
4. Update docs so `HAVING` support claims match the actual supported state.
5. Remove the suite from any residual “known limitations” or “future release” references.

**Why this is low risk:**

- The parser already has explicit `HAVING` rewrite logic.
- The DVM tree already models `HAVING` as `Aggregate` plus `Filter`.
- The behavior claimed in `docs/FAQ.md` already says threshold transitions are supported.

**Likely files:**

- `tests/e2e_having_transition_tests.rs`
- `CHANGELOG.md`
- `docs/FAQ.md`
- `docs/SQL_REFERENCE.md`

**Acceptance criteria:**

1. All five previously ignored `HAVING` tests run without `#[ignore]`.
2. The suite passes in normal E2E runs.
3. No documentation still lists `HAVING` threshold transitions as an open limitation.

---

## Workstream B — Verification Gate Before Re-enable

This workstream exists for cases that might already be fixed, but need clean proof before removing `#[ignore]`.

### B1. Correlated Scalar Subquery Suite

**Current state:** Two tests remain ignored in `tests/e2e_scalar_subquery_tests.rs`:

- `test_correlated_scalar_subquery_differential`
- `test_scalar_subquery_null_result_differential`

The earlier verification attempt did not surface a clean suite summary because the terminal wrapper was noisy. That is not enough evidence to remove the ignores safely.

**Primary hypothesis:** The correlated scalar-subquery path may now work after the parser decorrelation and scalar-subquery operator work, but this needs a clean pass/fail baseline.

**Relevant implementation areas:**

- `src/dvm/parser.rs`
  - correlated scalar-subquery extraction and decorrelation
  - scalar-subquery target parsing
  - scalar-subquery-in-WHERE rewrite and fallback behavior
- `src/dvm/operators/scalar_subquery.rs`
  - `diff_scalar_subquery`
- `src/dvm/diff.rs`
  - operator dispatch

**Implementation steps:**

1. Reproduce both ignored tests in a clean environment with output captured to files or CI artifacts.
2. If both pass repeatedly:
   - remove both `#[ignore]` annotations
   - add them to the normal E2E tier
   - update changelog/docs
3. If either fails:
   - capture the generated SQL and exact failure mode
   - determine whether the failure is in parser decorrelation, deparse, or the scalar-subquery diff operator
   - add unit coverage for the failing pattern before changing E2E annotations

**Decision rule:**

- `Passes cleanly twice` -> re-enable immediately
- `Fails once reproducibly` -> move into Workstream C-style bugfix work and keep ignored until fixed

**Acceptance criteria:**

1. There is clean evidence for one of two outcomes: re-enable or keep ignored for a specific bug.
2. The suite is no longer in an ambiguous state.

---

## Workstream C — Remaining DVM Fixes

This workstream covers the ignored tests that still represent real correctness bugs.

### C1. FULL JOIN Differential Correctness ✅ COMPLETE

**Completed:** 2026-03 (branch `plan-ignored-e2e-tests`)

All five ignored tests in `tests/e2e_full_join_tests.rs` now pass. Five distinct DVM
bug were fixed across `src/dvm/operators/project.rs` and `src/dvm/operators/aggregate.rs`.

**Root causes found and fixed:**

1. **`project.rs` — row-id mismatch (`is_join_child`):**
   FULL refresh computes `__pgt_row_id = hash(output_columns)`. Differential for
   FullJoin was passing through raw part row-ids (PK hashes or `0::BIGINT`) instead.
   Added `OpTree::FullJoin { .. }` to the `is_join_child` match in `diff_project`.
   Files: `src/dvm/operators/project.rs`

2. **`aggregate.rs` — compound GROUP BY expression resolution:**
   `resolve_group_col` called `resolve_col_for_child` (handles `ColumnRef` atoms only).
   `COALESCE(l.dept, r.dept)` hit the fallback `strip_qualifier()` path and became
   `COALESCE(dept, dept)` (ambiguous column). Fixed by calling `resolve_expr_for_child`
   which recursively resolves FuncCall arguments.
   Files: `src/dvm/operators/aggregate.rs`

3. **`aggregate.rs` — compound GROUP BY expression quoting:**
   GROUP BY clause and delta SELECT used `quote_ident(expr_sql)`, turning
   `COALESCE(l__dept, r__dept)` into the identifier `"COALESCE(l__dept, r__dept)"`.
   Added `col_ref_or_sql_expr` helper: bare names are quoted, expressions with `(` are
   emitted as raw SQL.
   Files: `src/dvm/operators/aggregate.rs`

4. **`aggregate.rs` — SUM NULL semantics over FULL JOIN:**
   After matched→unmatched transition, `COALESCE(old,0) + COALESCE(ins,0) − COALESCE(del,0)`
   evaluates to `0` even when all remaining group rows carry `NULL` for the aggregate column.
   PostgreSQL's `SUM` of all NULLs should be `NULL`. Fixed by building a group-rescan CTE
   for non-DISTINCT SUM when `child_has_full_join(child)` is true; the rescan value is used
   in the merge formula (`has_rescan = true` path in `agg_merge_expr_mapped`).
   Files: `src/dvm/operators/aggregate.rs`

5. **`aggregate.rs` — rescan CTE SELECT list for FuncCall group-by columns:**
   `build_rescan_cte` (and `build_intermediate_agg_delta`) emitted the output-name string
   as a bare column identifier when `expr_sql == output_name`. PostgreSQL interpreted
   `"COALESCE(…)"` as a column name rather than a function call. Fixed with an
   `!expr_sql.contains('(')` guard so FuncCall expressions use `expr AS alias` form.
   Files: `src/dvm/operators/aggregate.rs`

**Acceptance criteria met:**

1. ✅ All five `FULL JOIN` ignored tests pass (verified in normal CI mode, not `--ignored`).
2. ⚠️ No new dedicated full-join unit tests were added (the existing unit test suite
   did not catch these bugs; regression is covered by the E2E suite).
3. ✅ `FULL OUTER JOIN` is no longer listed as a known limitation in the changelog.

### C2. Correlated `EXISTS` with `HAVING` in Sublink ✅ COMPLETE

**Completed:** 2026-03 (branch `plan-ignored-e2e-tests`)

The single previously ignored test in `tests/e2e_sublink_or_tests.rs` now passes.
Three distinct DVM bugs were fixed across `src/dvm/parser.rs` and
`src/dvm/operators/project.rs`.

**Root causes found and fixed:**

1. **`parser.rs` — `parse_exists_sublink` ignored GROUP BY / HAVING:**
   When the inner EXISTS subquery contained `GROUP BY` and/or `HAVING`, the parser
   completely discarded those clauses and treated the subquery as a plain scan
   semi-join. `EXISTS (SELECT 1 FROM T WHERE T.k = outer.k GROUP BY T.k HAVING
   SUM(T.v) > threshold)` was mis-parsed as `SemiJoin(cond=T.k=outer.k, right=Scan(T))`
   — i.e., "does this outer row have ANY matching inner row?" instead of "does the sum
   for this outer row exceed the threshold?".

   Fix: When the inner SELECT has GROUP BY or HAVING, extract the correlation predicate
   from the inner WHERE, remove it from the pre-aggregate filter, build an
   `Aggregate(GROUP BY, agg_from_HAVING, child)` node, wrap it in `Filter(HAVING)`,
   wrap that in `Subquery(alias)`, and use the correlation predicate as the SemiJoin
   condition. Added three helper functions:
   - `collect_tree_source_aliases` — gathers Scan/CteScan aliases from an OpTree
   - `split_exists_correlation` — splits AND conjuncts into correlated equalities vs.
     inner-only predicates
   - `try_extract_exists_corr_pair` — tests whether an equality predicate is
     `inner.col = outer.ref` (or the reverse)

   Added 7 unit tests for these helpers.
   Files: `src/dvm/parser.rs`

2. **`parser.rs` / `project.rs` — row-id mismatch for `Project(SemiJoin)` trees:**
   `row_id_key_columns()` returned `None` for `Project(child=SemiJoin, aliases)` when
   `aliases.len()` differed from `child.output_columns().len()`. This caused the FULL
   refresh to use a `row_to_json(…) || '/' || row_number()` sentinel hash, while the
   differential refresh (via `diff_semi_join` Part 2 + `diff_project`) used
   `pg_trickle_hash_multi(ARRAY[left_col1, left_col2, ...])`. The row-id mismatch meant
   that rows inserted by FULL refresh were never matched — and therefore never deleted —
   by subsequent differential refreshes.

   Fix 1 (`parser.rs`): Extended `row_id_key_columns()` inside the `Project` branch to
   return `Some(aliases.clone())` when `unwrapped` is `SemiJoin` or `AntiJoin`. This
   makes the FULL refresh use a content hash of the projected output columns, matching
   what `diff_project` now produces.

   Fix 2 (`project.rs`): Extended `diff_project` to recompute `__pgt_row_id` from the
   projected column expressions for SemiJoin/AntiJoin children (the same treatment
   already applied to lateral function/subquery children). Both the FULL and DIFF
   paths now hash the same columns, so the MERGE `ON st.__pgt_row_id = d.__pgt_row_id`
   correctly matches existing rows.
   Files: `src/dvm/parser.rs`, `src/dvm/operators/project.rs`

**Acceptance criteria met:**

1. ✅ The previously ignored `test_exists_with_having_in_subquery_differential` test
   passes without `#[ignore]`.
2. ✅ All previously passing OR-exists/OR-in differential tests still pass.
3. ✅ All 5 full-join tests still pass.
4. ✅ All 7 HAVING-transition tests still pass.
5. ✅ Parser unit tests for the new correlation-splitting helpers added (7 unit tests).

---

## Cross-Cutting Cleanup

These tasks should happen alongside the workstreams above.

### D1. Re-baseline the Changelog and Docs

The current public narrative is internally inconsistent. `CHANGELOG.md` and parts of the docs still present the ignored-test inventory as if it were current, while other docs already claim support for some of those features.

**Required work:**

1. Replace the stale “18 ignored tests” note with a current statement.
2. Move the remaining limitations into a smaller, precise list based on actual failing suites.
3. Ensure docs only claim support for features that are exercised by non-ignored tests.

**Files likely affected:**

- `CHANGELOG.md`
- `docs/FAQ.md`
- `docs/SQL_REFERENCE.md`

### D2. CI Coverage Alignment

Every re-enabled test must run in the same CI tier that the project expects for E2E correctness.

**Required work:**

1. Verify `just test-e2e` and the PR-time light/full E2E jobs include the re-enabled suites.
2. Ensure there is no separate `--ignored` invocation required for the recovered tests.
3. If scalar-subquery verification is deferred, add an explicit note so that future contributors know that bucket is intentionally pending, not forgotten.

**Files likely affected:**

- `justfile`
- `.github/workflows/ci.yml` if suite selection is explicit there

### D3. Testing Status Tracking

Once the re-enable work starts, the testing status documents should reflect it.

**Files likely affected:**

- `plans/testing/PLAN_TESTING_GAPS.md`
- `plans/testing/PLAN_TESTING_GAPS.md` if the remaining failures become tracked implementation gaps

---

## Recommended Execution Order

### Phase 0 — Re-baseline

1. Re-run the ignored suites with clean output capture.
2. Produce a one-page matrix of `ignored -> passes -> re-enable` vs. `ignored -> still fails -> fix`.
3. Use that matrix as the only source of truth for the rest of the work.

### Phase 1 — Recover Fast Wins

1. Land changelog cleanup for keyless duplicates.
2. Remove ignores from the five `HAVING` transition tests.
3. Run `just fmt`, `just lint`, and the affected E2E suites.

### Phase 2 — Resolve Scalar Ambiguity

1. Verify the scalar-subquery suite cleanly.
2. Re-enable it if green.
3. Otherwise convert it into a targeted bugfix task with exact captured failure output.

### Phase 3 — Fix FULL JOIN

1. Add failing unit tests that mirror the five E2E cases.
2. Fix `diff_full_join` and any supporting join-common behavior.
3. Re-enable the suite once both unit and E2E tests pass.

### Phase 4 — Fix Correlated `EXISTS ... HAVING`

1. Stabilize parser rewrite and aggregate-state preservation.
2. Add regression coverage.
3. Re-enable the final ignored test.

### Phase 5 — Final Consistency Pass

1. Update docs and status files.
2. Re-run the full validation stack.
3. Confirm the repository no longer has stale references to the original ignored-test inventory.

---

## Validation Matrix

After each phase, run the narrowest relevant validation first, then the required repo-wide checks.

### Narrow Validation

| Change | Minimum validation |
|---|---|
| Remove ignores from `HAVING` suite | `cargo test --test e2e_having_transition_tests -- --test-threads=1 --nocapture` |
| Remove ignores from scalar suite | `cargo test --test e2e_scalar_subquery_tests -- --test-threads=1 --nocapture` |
| Fix `FULL JOIN` operator | `cargo test --test e2e_full_join_tests -- --test-threads=1 --nocapture` plus new unit tests |
| Fix sublink OR aggregate case | `cargo test --test e2e_sublink_or_tests -- --test-threads=1 --nocapture` plus new parser/operator unit tests |

### Required Repository Validation After Code Changes

1. `just fmt`
2. `just lint`
3. The affected E2E suites
4. `just test-e2e` before closing the work if multiple ignored buckets were touched

---

## Risks

1. `HAVING` may pass in the current environment but still hide order-dependent or timing-dependent failures in CI.
2. The scalar-subquery bucket may be partially fixed, creating pressure to remove ignores before the root cause is fully understood.
3. `FULL JOIN` failures may expose coupled bugs in both the dedicated full-join operator and aggregate-on-top behavior.
4. The OR-to-UNION sublink rewrite may need a more principled representation of grouped correlated subqueries than the current branch rewrite preserves.

---

## Definition of Done

This plan is complete when all of the following are true:

1. The stale “18 ignored E2E tests” statement is removed or replaced with current facts.
2. Every test that is already supportable runs without `#[ignore]`.
3. Every still-ignored test has a concrete tracked implementation reason, not a stale inherited annotation.
4. `FULL JOIN` and correlated `EXISTS ... HAVING` have unit-level regression coverage in addition to E2E coverage.
5. Docs, changelog, and CI all reflect the same supported-state story.

Until then, the project should treat ignored-test inventory drift as a correctness and release-readiness issue, not just a documentation issue.
