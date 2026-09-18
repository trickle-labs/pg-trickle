# TESTING_GAPS_2.md — Regression Risk Analysis & Test Hardening Plan

**Date:** 2026-03-28  
**Branch:** pre-release-0.12.0-b  
**Scope:** Deep audit of all six test tiers to identify structural weaknesses
where regressions are most likely to escape detection.

This plan complements the earlier feature-level coverage work with a
focus on **regression risk reduction**: scenarios where correct-today code can
break tomorrow without any test catching it.

---

## Table of Contents

1. [Current State Summary](#1-current-state-summary)
2. [HIGH — Window Function DVM Operator Testing](#2-high--window-function-dvm-operator-testing)
3. [HIGH — Join Multi-Cycle Correctness](#3-high--join-multi-cycle-correctness)
4. [HIGH — Differential ≡ Full Equivalence Validation](#4-high--differential--full-equivalence-validation)
5. [MEDIUM — refresh.rs Core Path Unit Tests](#5-medium--refreshrs-core-path-unit-tests)
6. [MEDIUM — Lateral/Recursive Operator Execution Tests](#6-medium--lateralrecursive-operator-execution-tests)
7. [MEDIUM — Timeout, Cancellation & Failure Recovery](#7-medium--timeout-cancellation--failure-recovery)
8. [MEDIUM — Source Table Schema Evolution](#8-medium--source-table-schema-evolution)
9. [LOW — Correctness Gate Scale & Sustained Cycles](#9-low--correctness-gate-scale--sustained-cycles)
10. [LOW — Test Infrastructure Improvements](#10-low--test-infrastructure-improvements)
11. [Implementation Order](#11-implementation-order)
12. [Appendix: Current Coverage Inventory](#appendix-current-coverage-inventory)

---

## 1. Current State Summary

### Strengths

The test suite is substantial (68 files, ~700+ tests, 80K+ lines) with solid
foundations:

- **Testcontainers isolation** — each test gets a fresh DB, no state leakage.
- **Multiset equality** via `EXCEPT ALL` — catches extra rows, missing rows,
  and duplicate count mismatches including NULLs.
- **Property-based testing** — proptest in `wal_decoder.rs`, `dvm/mod.rs`, and
  `semi_join.rs` (200 cases) with persisted regression seeds.
- **TPC-H correctness gate** — 5 queries with zero-tolerance binary pass/fail.
- **Multi-tier architecture** — unit (fast) → DVM execution → integration → E2E
  provides fast feedback plus real-DB validation.
- **All 20 DVM operators** have unit test modules.
- **SQLancer fuzzing** — crash oracle and query equivalence oracle.

### Weaknesses Identified

| Risk   | Area                                  | Gap Description                                              |
|--------|---------------------------------------|--------------------------------------------------------------|
| HIGH   | Window function DVM deltas            | ~5 unit tests, 0 DVM execution tests                        |
| HIGH   | Join multi-cycle correctness          | E2E join tests are INSERT-only, no UPDATE/DELETE cycles      |
| HIGH   | Differential ≡ Full equivalence       | Only validated for CTEs; joins/aggregates lack this          |
| MEDIUM | `refresh.rs` core unit tests          | Only helpers/enums tested; MERGE template untested           |
| MEDIUM | Lateral/recursive operator execution  | 4-6 unit tests each, 0 execution tests                      |
| MEDIUM | Timeout/cancellation during refresh   | 0 tests for statement_timeout, pg_cancel_backend             |
| MEDIUM | Source table schema evolution          | Partial DDL tests; type changes & column renames thin        |
| LOW    | Correctness gate scale                | SF-0.01 (~6K rows), 1 mutation cycle                        |
| LOW    | Polling helper fragmentation          | 5 different wait-for-refresh implementations                 |

---

## 2. HIGH — Window Function DVM Operator Testing

### Problem

`src/dvm/operators/window.rs` has ~5 unit tests covering basic partition
detection. There are no DVM execution tests (real SQL generation against
PostgreSQL) for window functions. E2E window tests exist but test operations in
isolation — no sequential multi-cycle chains.

Window functions involve complex frame calculations (`ROWS BETWEEN`, `RANGE
BETWEEN`), partition detection, and delta propagation. A subtle regression in
`ROW_NUMBER()` or `RANK()` delta logic could corrupt ordering silently.

### Tests to Add

#### DVM Execution Tests (`tests/dvm_window_tests.rs` — new file)

```
test_window_row_number_insert_delta
test_window_row_number_delete_delta
test_window_row_number_update_delta
test_window_rank_insert_delta
test_window_rank_dense_rank_insert_delta
test_window_lag_lead_insert_delta
test_window_lag_lead_delete_delta
test_window_sum_over_partition_insert_delta
test_window_sum_over_partition_delete_delta
test_window_ntile_insert_delta
test_window_frame_rows_between_insert_delta
test_window_frame_range_between_insert_delta
test_window_multiple_partitions_mixed_dml
test_window_null_in_partition_by
test_window_null_in_order_by
```

Each test should:
1. Create source table + populate baseline data
2. Create a view with the window function
3. Generate differential SQL via DVM engine
4. Execute baseline query → capture expected result
5. Apply DML (INSERT/UPDATE/DELETE) to source
6. Execute differential SQL → apply delta
7. Assert result matches fresh execution of the defining query

#### Multi-Cycle E2E Tests (extend `e2e_window_tests.rs`)

```
test_window_multi_cycle_row_number_differential
test_window_multi_cycle_rank_differential
test_window_multi_cycle_lag_lead_differential
```

Pattern: 5 cycles of INSERT→verify→UPDATE→verify→DELETE→verify→mixed→verify→
idempotent→verify (matching `e2e_multi_cycle_tests.rs` pattern).

### Acceptance Criteria

- [ ] 15+ DVM execution tests for window functions pass
- [ ] 3+ multi-cycle E2E window tests pass
- [ ] All tests run in `just test-dvm` and `just test-light-e2e` respectively

---

## 3. HIGH — Join Multi-Cycle Correctness

### Problem

`e2e_join_tests.rs` primarily tests single-shot INSERT scenarios. There are no
tests that chain UPDATE→refresh→verify→DELETE→refresh→verify for joins. Critical
scenarios untested:

- **Update on join key** — a row changes which table it matches in a JOIN
- **Delete from one side** of a LEFT/FULL join — result transitions from
  matched to NULL-padded
- **Concurrent changes on both sides** of a join in the same cycle
- **Deep join chains** (≥4 tables) with per-leaf snapshot reconstruction

The `e2e_multi_cycle_tests.rs` file has `test_multi_cycle_join_differential`
(4 cycles), but it only covers INNER JOIN. LEFT, RIGHT, FULL, and
CROSS joins lack multi-cycle coverage.

### Tests to Add

#### Extend `e2e_join_tests.rs` or `e2e_multi_cycle_tests.rs`

```
test_multi_cycle_left_join_differential
    — 5 cycles: INSERT matched→verify, INSERT unmatched→verify,
      UPDATE makes matched→unmatched→verify, DELETE matched→verify,
      DELETE unmatched→verify

test_multi_cycle_full_join_differential
    — 5 cycles: INSERT both sides→verify, DELETE left only→verify,
      DELETE right only→verify, UPDATE join key→verify,
      INSERT re-match→verify

test_multi_cycle_right_join_differential
    — Mirror of left join test

test_multi_cycle_join_key_update
    — Focused test: UPDATE that changes the join key column,
      row moves from matching one partner to another

test_multi_cycle_join_both_sides_changed
    — Concurrent INSERT on left + DELETE on right in same cycle

test_deep_join_chain_4_tables_differential
    — A JOIN B JOIN C JOIN D; change each table in separate cycles;
      verify final result matches defining query each time

test_join_null_key_transitions
    — INSERT row with NULL join key → verify NULL-padded result
    — UPDATE NULL→non-NULL → verify row now matches
    — UPDATE non-NULL→NULL → verify row becomes unmatched
```

### Acceptance Criteria

- [ ] 7+ multi-cycle join tests pass
- [ ] Tests cover LEFT, RIGHT, FULL JOIN types
- [ ] At least one test exercises UPDATE on join key column
- [ ] Tests use `assert_st_matches_query` after every refresh

---

## 4. HIGH — Differential ≡ Full Equivalence Validation

### Problem

The strongest correctness property of the system — "differential refresh
produces the same result as full refresh" — is only explicitly validated for
CTEs (`test_cte_differential_matches_full_results`) and one window test. Joins
and aggregates lack this validation.

`assert_st_matches_query` compares against the defining query but doesn't
distinguish whether differential or full refresh was used. A bug where
differential refresh silently falls back to full would pass all existing tests.

### Tests to Add

#### New file: `tests/e2e_diff_full_equivalence_tests.rs`

This suite should be a systematic cross-cutting validation. For each operator
class, the pattern is:

1. Create ST with `refresh_mode => 'DIFFERENTIAL'`
2. Populate baseline + initial full refresh
3. Apply mixed DML (INSERT + UPDATE + DELETE) in 5 cycles
4. After each cycle, capture the ST result
5. Do a manual `SELECT pgtrickle.refresh('...', mode => 'FULL')`
6. Assert differential result == full result (multiset equality)
7. Verify from `pgt_refresh_history` that differential mode was **actually used**
   (not a silent fallback)

```
test_diff_full_equivalence_inner_join
test_diff_full_equivalence_left_join
test_diff_full_equivalence_full_join
test_diff_full_equivalence_aggregate_sum_count
test_diff_full_equivalence_aggregate_avg_stddev
test_diff_full_equivalence_window_row_number
test_diff_full_equivalence_window_rank
test_diff_full_equivalence_cte_recursive
test_diff_full_equivalence_lateral_subquery
test_diff_full_equivalence_intersect
test_diff_full_equivalence_except
test_diff_full_equivalence_distinct_on
test_diff_full_equivalence_having_clause
test_diff_full_equivalence_three_table_join
```

Each test should also assert:
- `last_refresh_mode = 'DIFFERENTIAL'` in catalog (not silent FULL fallback)
- Row count is non-trivial (≥10 rows after mutations)

### Acceptance Criteria

- [ ] 14 tests covering all major operator classes
- [ ] Each test does 5 mutation cycles
- [ ] Each cycle validates differential result == full result
- [ ] Each cycle confirms differential mode was actually used
- [ ] Tests run in `just test-light-e2e`

---

## 5. MEDIUM — `refresh.rs` Core Path Unit Tests

### Problem

`src/refresh.rs` has ~20 unit tests but they only cover `RefreshAction` enum
conversions and validation checks (e.g., "unpopulated ST rejected"). The actual
differential refresh execution path — MERGE SQL template generation, cache
warmup, delta application — has zero isolated unit tests. All coverage comes
from E2E tests.

This means a regression in MERGE template generation requires 5+ minutes of
Docker build + test to detect, rather than seconds in `just test-unit`.

### Tests to Add

Extract pure functions from the refresh path and add unit tests:

```
test_merge_sql_template_basic_pk_table
    — Verify generated MERGE SQL for a simple (id PK, val TEXT) table

test_merge_sql_template_composite_pk
    — Verify MERGE with (a, b) composite PK

test_merge_sql_template_keyless_table
    — Verify MERGE for keyless table (no dedup, uses __pgt_row_id)

test_merge_sql_template_with_partition_predicate
    — Verify __PGT_PART_PRED__ placeholder resolution

test_merge_sql_template_column_name_escaping
    — Column names that are SQL keywords ("order", "group", "select")

test_merge_sql_template_wide_table
    — 50+ columns; verify IS DISTINCT FROM clause completeness

test_cache_invalidation_on_query_change
    — Verify thread-local cache is invalidated when query definition changes

test_differential_skips_when_no_changes
    — Verify NO_DATA outcome when change buffer is empty
```

### Refactoring Required

The MERGE template generation is currently embedded in the SPI-interacting
`execute_differential_refresh`. To unit-test it:

1. Extract `fn build_merge_sql(columns: &[Column], pk_cols: &[String],
   partition_pred: Option<&str>) -> String` as a pure function
2. Unit test the pure function with various column configurations
3. Keep the SPI-dependent orchestration as-is

### Acceptance Criteria

- [ ] MERGE template generation is a testable pure function
- [ ] 8+ unit tests cover PK, composite PK, keyless, partitioned, wide tables
- [ ] Tests run in `just test-unit` (no DB required)

---

## 6. MEDIUM — Lateral/Recursive Operator Execution Tests

### Problem

Three DVM operators have only minimal unit tests and zero execution tests:

| Operator              | Unit Tests | Execution Tests |
|-----------------------|------------|-----------------|
| `lateral_subquery.rs` | ~6         | 0               |
| `lateral_function.rs` | ~4         | 0               |
| `recursive_cte.rs`    | ~4         | 0               |

These operators have complex semantics (NULL-padded lateral columns, SRF
expansion, recursion termination) that are difficult to validate with unit tests
alone.

### Tests to Add

#### DVM Execution Tests

```
# Lateral subquery
test_lateral_subquery_insert_delta
test_lateral_subquery_delete_delta
test_lateral_subquery_with_aggregate_outer

# Lateral function (SRF)
test_lateral_function_generate_series_insert_delta
test_lateral_function_unnest_insert_delta
test_lateral_function_delete_delta

# Recursive CTE
test_recursive_cte_insert_into_base_case
test_recursive_cte_delete_from_base_case
test_recursive_cte_depth_change_after_update
```

#### Multi-Cycle E2E Tests

```
test_multi_cycle_lateral_join_differential
test_multi_cycle_recursive_cte_differential
```

### Acceptance Criteria

- [ ] 9 DVM execution tests for lateral/recursive operators
- [ ] 2 multi-cycle E2E tests
- [ ] Tests run in `just test-dvm` and `just test-light-e2e`

---

## 7. MEDIUM — Timeout, Cancellation & Failure Recovery

### Problem

Zero tests exist for:
- `statement_timeout` during refresh (what state is the ST left in?)
- `pg_cancel_backend()` during an active refresh
- Lock timeout when another transaction holds a conflicting lock
- OOM / `temp_file_limit` exceeded during a large MERGE

In production, these are common failure modes. If the extension doesn't handle
them gracefully, stream tables could be left in an inconsistent state
(partially applied delta, stale `is_refreshing` flag, etc.).

### Tests to Add

#### `tests/e2e_failure_recovery_tests.rs` (new file)

```
test_statement_timeout_during_refresh_recovers
    — SET statement_timeout = '100ms' for the refresh session
    — Trigger refresh on a complex ST (ensure it takes >100ms)
    — Verify: ST status is not REFRESHING after timeout
    — Verify: next refresh succeeds and produces correct data

test_cancel_backend_during_refresh_recovers
    — Start refresh in background (async)
    — pg_cancel_backend() on the refresh backend PID
    — Verify: ST is not corrupt; next refresh succeeds

test_lock_timeout_during_refresh
    — Hold an ACCESS EXCLUSIVE lock on the ST table
    — Attempt refresh with lock_timeout = '100ms'
    — Verify: refresh fails gracefully, no dead state

test_refresh_after_source_table_truncate
    — TRUNCATE source table (triggers reinit path)
    — Verify: next refresh produces empty result (not stale data)

test_refresh_after_repeated_failures_fuse_activates
    — Create ST with query that references a function that errors
    — Trigger several refresh cycles
    — Verify: consecutive_errors increments
    — Verify: ST enters SUSPENDED after threshold
    — Fix the function, RESUME, verify recovery

test_concurrent_drop_during_refresh
    — Start refresh in background
    — DROP STREAM TABLE concurrently
    — Verify: no orphaned change buffer tables or catalog entries
```

### Acceptance Criteria

- [ ] 6 failure recovery tests pass
- [ ] Tests verify both error handling AND successful recovery after error
- [ ] No test leaves the DB in an inconsistent state

---

## 8. MEDIUM — Source Table Schema Evolution

### Problem

`e2e_ddl_event_tests.rs` covers some DDL scenarios (add column, drop unused
column, alter type triggers reinit). However, several important schema evolution
scenarios are missing or thin:

- Column **rename** on source table (does ST detect and reinit?)
- Column **type change** that preserves data but changes OID
  (e.g., `varchar(50)` → `varchar(100)`)
- Adding a **NOT NULL** constraint to a source column
- Changing a column's **default value**
- Adding/dropping **CHECK** constraints
- **Concurrent DDL + DML** on source table during refresh

### Tests to Add

Extend `e2e_ddl_event_tests.rs`:

```
test_rename_source_column_triggers_reinit
    — ALTER TABLE src RENAME COLUMN val TO value
    — Verify: ST detects change and reinitializes
    — Verify: next refresh uses new column name

test_widen_varchar_type_benign
    — ALTER TABLE src ALTER COLUMN val TYPE varchar(200)
    — Verify: ST continues to work without reinit (if type is compatible)

test_add_not_null_constraint_benign
    — ALTER TABLE src ALTER COLUMN val SET NOT NULL
    — Verify: ST not affected (constraint doesn't change data)

test_drop_referenced_column_errors_clearly
    — ALTER TABLE src DROP COLUMN val (where ST query uses val)
    — Verify: clear error message, ST suspended, not silently broken

test_concurrent_ddl_and_dml_isolation
    — In one transaction: ALTER TABLE src ADD COLUMN new_col INT
    — In another transaction: INSERT INTO src (existing cols)
    — Verify: ST handles the interleaving correctly
```

### Acceptance Criteria

- [ ] 5 schema evolution tests pass
- [ ] Tests verify both reinit detection and error messaging
- [ ] No silent data corruption on schema changes

---

## 9. LOW — Correctness Gate Scale & Sustained Cycles

### Problem

The TPC-H correctness gate (`e2e_correctness_gate_tests.rs`) runs at SF-0.01
(~6K rows) with only 1 mutation cycle (RF1 bulk INSERT + RF2 bulk DELETE). This
validates basic correctness but doesn't stress:

- Sustained accuracy over many cycles (drift accumulation)
- Larger dataset WHERE hash collisions and edge cases are more likely
- Performance regressions in differential refresh at moderate scale

### Improvements

```
# Extend correctness gate with sustained cycles
test_correctness_gate_q01_5_cycles
test_correctness_gate_q03_5_cycles
test_correctness_gate_q06_5_cycles
test_correctness_gate_q13_5_cycles
test_correctness_gate_q14_5_cycles
```

Each runs 5 rounds of RF1+RF2 with verification after each round.

For daily CI (not PR): bump to SF-0.1 (~60K rows) to increase chance of
catching edge cases.

### Acceptance Criteria

- [ ] 5-cycle variants added (can be `#[ignore]` for PR CI)
- [ ] SF-0.1 variant available for daily/manual dispatch
- [ ] No correctness drift over 5 cycles

---

## 10. LOW — Test Infrastructure Improvements

### 10a. Polling Helper Consolidation

There are 5+ different implementations of "wait for refresh" polling across
test files:

- `wait_for_refresh_cycle` in `e2e_dag_autorefresh_tests.rs`
- `wait_for_scheduler` in `tests/e2e/mod.rs`
- `wait_for_auto_refresh` in `tests/e2e/mod.rs`
- Custom loops in `e2e_safety_tests.rs`, `e2e_circular_tests.rs`

**Action:** Consolidate into a single `wait_for_condition` helper:

```rust
/// Poll a boolean SQL expression until it returns true or timeout expires.
/// Uses exponential backoff starting at 100ms, capped at 2s.
pub async fn wait_for_condition(
    &self,
    sql: &str,
    timeout: Duration,
    label: &str,
) -> bool { ... }
```

Then re-implement `wait_for_auto_refresh` and `wait_for_scheduler` as thin
wrappers around `wait_for_condition`.

### 10b. Assert Column Types in `assert_sets_equal`

Currently `assert_sets_equal` does multiset comparison via `EXCEPT ALL` but
does not validate that column types match between the two sides. A type
mismatch (e.g., `TEXT` vs `NUMERIC`) would be silently cast by PostgreSQL.

**Action:** Add an optional `assert_types_match` parameter that queries
`pg_catalog.pg_attribute` for both relations and compares `atttypid` arrays.

### 10c. Compiler Warning on Unused `wait_for_*` Return Values

The `wait_for_auto_refresh` and `wait_for_scheduler` helpers return `bool` but
the Rust compiler doesn't warn when the return value is unused (it's not marked
`#[must_use]`).

**Action:** Add `#[must_use = "check whether the refresh actually happened"]`
to both helpers.

### Acceptance Criteria

- [ ] Single `wait_for_condition` helper with backoff
- [ ] Existing wait helpers refactored as thin wrappers
- [ ] `#[must_use]` on all boolean-returning wait helpers
- [ ] Optional type-match assertion in `assert_sets_equal`

---

## 11. Implementation Order

Priority order balances regression risk against implementation effort:

| Phase | Items | Effort  | Impact |
|-------|-------|---------|--------|
| **1** | §10c: `#[must_use]` annotations | 30 min | Prevents future silent test bugs |
| **2** | §3: Join multi-cycle tests | 1 day | Closes biggest correctness gap |
| **3** | §4: Diff ≡ Full equivalence suite | 1-2 days | Strongest regression safety net |
| **4** | §2: Window DVM execution tests | 1 day | Closes thinnest operator coverage |
| **5** | §7: Failure recovery E2E tests | 1 day | Production resilience |
| **6** | §6: Lateral/recursive execution tests | 1 day | DVM engine coverage |
| **7** | §8: Schema evolution tests | 0.5 day | DDL robustness |
| **8** | §5: refresh.rs MERGE template unit tests | 1 day | Requires refactoring |
| **9** | §10a-b: Test infrastructure polish | 0.5 day | Developer experience |
| **10** | §9: Correctness gate scale | 0.5 day | Confidence at scale |

---

## Appendix: Current Coverage Inventory

### Unit Test Coverage by Source File

| File | Unit Tests | Assessment |
|------|-----------|------------|
| `src/api.rs` | ~100 | Strong (helpers + proptest); main CRUD untestable without DB |
| `src/catalog.rs` | ~14 | Thin — almost entirely stubbed; depends on integration tests |
| `src/cdc.rs` | ~40 | Good helpers; trigger execution path untested |
| `src/config.rs` | ✅ | Adequate |
| `src/dag.rs` | ~20 | Core paths covered; complex diamonds/policies thin |
| `src/diagnostics.rs` | 0 | No tests |
| `src/dvm/diff.rs` | ~40 | SQL generation tested; delta application semantics untested |
| `src/dvm/mod.rs` | ✅ | Good + proptest |
| `src/dvm/parser.rs` | ~200 | Excellent across 11 modules |
| `src/dvm/row_id.rs` | ✅ | Adequate |
| `src/error.rs` | ~12 | Classification tested; message quality untested |
| `src/hash.rs` | ✅ | Adequate |
| `src/hooks.rs` | ~50 | DDL classification strong; trigger execution untested |
| `src/lib.rs` | ✅ | Adequate |
| `src/monitor.rs` | ✅ | Adequate |
| `src/refresh.rs` | ~20 | Helpers only; core execution path untested |
| `src/scheduler.rs` | ~50 | Utilities tested; main loop/locking untested |
| `src/shmem.rs` | ✅ | Adequate |
| `src/version.rs` | ✅ | Adequate |
| `src/wal_decoder.rs` | ✅ | Good + proptest; rare edge cases (slot loss, schema change during polling) thin |

### DVM Operator Test Depth

| Operator | Unit Tests | Execution Tests | Risk |
|----------|-----------|-----------------|------|
| `scan.rs` | ~30 | N/A (base) | LOW |
| `filter.rs` | ~15 | Implicit | LOW |
| `project.rs` | ~13 | Implicit | LOW |
| `join.rs` | ~20 | 8 | MEDIUM — multi-cycle gap |
| `full_join.rs` | ~14 | 5 | MEDIUM — NULL propagation |
| `outer_join.rs` | ~18 | 2 | LOW |
| `semi_join.rs` | ~11 + 200pt | 6 | LOW |
| `anti_join.rs` | ✅ | Implicit | LOW |
| `aggregate.rs` | ~20 | 50+ | LOW |
| `window.rs` | ~5 | 0 | **HIGH** |
| `distinct.rs` | ~11 | Implicit | LOW |
| `union_all.rs` | ~14 | Implicit | LOW |
| `intersect.rs` | ~12 | Implicit | LOW |
| `except.rs` | ~16 | Implicit | LOW |
| `cte_scan.rs` | ~6 | Implicit | LOW |
| `subquery.rs` | ~7 | 0 | MEDIUM |
| `scalar_subquery.rs` | ~8 | Implicit | LOW |
| `lateral_subquery.rs` | ~6 | 0 | MEDIUM |
| `lateral_function.rs` | ~4 | 0 | MEDIUM |
| `recursive_cte.rs` | ~4 | 0 | MEDIUM |

### E2E Mutation Cycle Depth

| Area | Single-Shot (INSERT only) | Multi-Cycle (4-5 rounds) | Diff≡Full |
|------|---------------------------|--------------------------|-----------|
| Aggregates | Many | 5 tests in multi_cycle | No |
| Joins (INNER) | Many | 1 test in multi_cycle | No |
| Joins (LEFT/RIGHT/FULL) | Some | **None** | No |
| Windows | Many | **None** | 1 test |
| CTEs | Many | Some | 1 test |
| Lateral | Some | **None** | No |
| Set operations | Some | **None** | No |

### Tests Skipped on PR CI

~91 tests are `#[ignore]` and only run on push-to-main, daily schedule, or
manual dispatch:

- TPC-H suite (13 tests)
- Upgrade E2E (8 tests)
- DAG benchmarks (13 tests)
- CDC write overhead (6 tests)
- Various others

This is acceptable for PR velocity but means regressions in these areas are
detected with a delay of hours to days.
