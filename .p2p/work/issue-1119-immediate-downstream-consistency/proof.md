# PROVEN: Issue #1119 immediate downstream consistency

Requirements: 8/8  
Counterexamples tested: 9 named acceptance E2E cases

**Contract:** `work/issue-1119-immediate-downstream-consistency.md`, v1  
**Contract snapshot SHA-256:** `c7ecbeb273d2fd8ed8ef6ef3cc31fd0f2128550049a1daea93896bf7626592b8`  
**Parent context:** None  
**Candidate:** `snapshot:sha256:08f36577aca0522020d99d2bad996917b98746f08d0875bb06fb3596176405c9`  
**Reconstructed candidate:** `/private/tmp/pg-trickle-issue1119-candidate-final-08f36577`, 1,556 manifest entries  
**Comparison base:** `2dfe8dae90452120cf6760b71dd0def8c241756b`, exported at `/private/tmp/pg-trickle-issue1119-base`  
**Candidate stability:** Unchanged during this proof refresh. The exact saved snapshot was revalidated against the current product tree; no product or contract files were written.  
**Contract stability:** Unchanged. The contract hash matches the candidate record. Its bound inputs also match: `AGENTS.md` SHA-256 `1cf7139e5cb64e3f3c6d33908fe9870e5c99455a6e77987e4a6aee61833cdd87` and `work/sources/issue-1119.md` SHA-256 `d9abb79c4500e309a71a98cbf9329479b042cac81ede57af24a51833512cb10f`.  
**Verification context:** macOS host with PostgreSQL 18 custom E2E images/Testcontainers. This refresh reused retained behavioral evidence and reran candidate identity validation; it made no product or contract changes.

## Outcome

The exact candidate establishes all eight contract requirements. Behavioral checks use exact result comparisons against defining queries or base-table-derived queries, plus literal state and history assertions.

The retained 92-test E2E and upgrade logs predate the final candidate snapshot. I compared the final snapshot with the immediately preceding snapshot: the only difference is in `tests/e2e_ivm_tests.rs`, where five successful R5 retry comparisons now run through a fresh connection. The product code and test functions relevant to R1–R4 and R6–R8 are unchanged. The final candidate’s updated R5 test was run separately and passed 1/1. The broad E2E and upgrade suites were not rerun on the final snapshot.

The current [review report](review.md) is `REVIEWED` for this exact candidate, contract, and comparison base, with no material change-required findings (SHA-256 `7e9f93f37346ac6cc73bf240cb6a79888e1746f626296e4fad1b5fba32fd63df`). Its note about the then-current proof refers to the older proof that was present when the review was written. The refreshed proof and current review now bind the same candidate and agreement.

## Pair and identity refresh

On 2026-09-27 I revalidated `candidate.json` against the exact work item and current product tree with `p2p_filesystem.py validate --base 2dfe8dae90452120cf6760b71dd0def8c241756b work/issue-1119-immediate-downstream-consistency.md`; it exited 0. The durable result is `evidence/issue-1119-proof-final-validation.md` (SHA-256 `570e5336d10774cd0a12c12cf547dc9115ed816a8b3a4c5912c185ffcf87ace6`). The check confirms the candidate digest, all 1,556 manifest paths/bytes/modes, work-item hash, binding inputs, fixed base, and empty index.

The existing behavioral evidence remains bound to this snapshot. The unit (2,587 passed), integration (161 passed), related E2E (92 passed), and upgrade (33 passed) logs were run on the immediate predecessor snapshot. The final candidate differs there only in R5's successful retry assertions using a fresh connection; the production code and the R1–R4/R6–R8 test functions are unchanged. The final R5 test was run on this exact snapshot and passed 1/1. Broader E2E and upgrade suites were not rerun on the final snapshot. No test result has been promoted beyond what its recorded candidate establishes.

## Requirement verdicts

| ID | Observation and independent oracle | Evidence reference | Verdict |
|---|---|---|---|
| R1 | `test_ivm_full_refresh_preserves_immediate_descendants` checks both IMMEDIATE levels against their defining queries before commit and after reconnect; the scheduled case witnesses completed scheduler FULL history and checks both descendants at a stable checkpoint. Expected results come from the replacement upstream. | Final snapshot `tests/e2e_ivm_tests.rs:771,879`; retained `issue-1119-repair-final4-related-e2e.log:28,35` reports both PASS. The tests use exact symmetric `EXCEPT ALL` result comparison. | **PROVEN** |
| R2 | The manual suppression test asserts zero audit-trigger writes and exact child contents, including empty replacement. The scheduled R1 case checks the same side-effect boundary. A separate case proves suppression when the application trigger uses a reserved prefix and is the only application trigger. | Final snapshot `tests/e2e_user_trigger_tests.rs:600,668`; retained E2E log `:86,88` reports both suppression cases PASS. Scheduled R1 case PASS at `issue-1119-repair-final4-related-e2e.log:35`. | **PROVEN** |
| R3 | The test compares the literal pre-operation trigger identity-to-`tgenabled` mapping after manual and scheduled FULL refresh for ordinary, disabled, replica, and always states; it also asserts maintenance-trigger operation through the descendant results. | Final snapshot `tests/e2e_user_trigger_tests.rs:769`; retained E2E log `issue-1119-repair-final4-related-e2e.log:92` reports PASS. R5 separately checks trigger state after rollback and injected failures. | **PROVEN** |
| R4 | The truncate test checks duplicate and NULL payloads through IMMEDIATE, DIFFERENTIAL, and FULL capture. After truncate, the aggregate still returns its zero-count row; captured differential-buffer assertions distinguish D `(row_count=3,total=2)` from I `(row_count=0,total=NULL)`. It refreshes two consumers separately, covers repeated empty truncate and a later duplicate batch, and compares exact results to defining queries. | Final snapshot `tests/e2e_ivm_tests.rs:999`; retained E2E log `issue-1119-repair-final4-related-e2e.log:38` reports PASS. | **PROVEN** |
| R5 | The final test covers explicit FULL and TRUNCATE rollback plus deterministic CHECK-constraint failures. After rollback/failure it opens a fresh connection and checks complete row/buffer snapshots, frontiers, and trigger states. After removing each fault, retry results for upstream, IMMEDIATE, and deferred consumers are compared exactly from fresh connections. | Final snapshot `tests/e2e_ivm_tests.rs:119,1230`; `issue-1119-r5-final-e2e.log:7,9` reports `test_ivm_replacement_failure_rolls_back_and_recovers ... ok`, 1 passed, 0 failed. | **PROVEN** |
| R6 | The first eligible deferred refresh and its repeat are recorded as DIFFERENTIAL with `was_full_fallback=false`; exact defining-query comparisons show the captured changes were consumed without relying on a FULL rebuild or applying a batch twice. | Final snapshot `tests/e2e_ivm_tests.rs:999`; retained E2E log `issue-1119-repair-final4-related-e2e.log:38` reports the R4/R6 test PASS. | **PROVEN** |
| R7 | The documented recovery procedure distinguishes installing code, applying the applicable migration, and repairing existing state; it gives dependency-order repair and already-installed-version guidance. The E2E test creates a stale multi-level cascade, repairs it in order, compares each level with base-derived queries, then checks a subsequent write before commit. | Final snapshot `docs/UPGRADING.md:5` and `tests/e2e_repair_tests.rs:201`; retained E2E log `issue-1119-repair-final4-related-e2e.log:77` reports PASS. | **PROVEN** |
| R8 | The upgrade test performs a real 0.108.0 → 0.108.1 extension update, checks the upgraded view shape/types and retained grant, asserts healthy `broken_immediate_tables=0`, missing/disabled trigger states as broken and CRITICAL, then confirms repair returns the count to zero. It compares the upgraded view with a fresh candidate installation. | Final snapshot `tests/e2e_upgrade_tests.rs:852`; retained `issue-1119-repair-final2-upgrade.log:624,631` reports the named test PASS. The run summary reports 33 tests passed. | **PROVEN** |

## Additional verification

Retained checks report:

- Unit tests: 2,587 passed, 0 failed (`issue-1119-repair-final2-unit.log:2595`).
- Integration tests: 161 passed, 0 skipped (`issue-1119-repair-final-integration.log:169`).
- Related E2E suite: 92 passed, 0 skipped (`issue-1119-repair-final4-related-e2e.log:121`).
- Final R5 E2E check: 1 passed, 0 failed (`issue-1119-r5-final-e2e.log:9`).
- Formatting, Clippy with `-D warnings`, security and privilege checks, docs lint, SQL-builder audit, fuzz-target inventory, and monitoring-contract checks pass in `issue-1119-repair-final2-fmt-lint.log`, `issue-1119-r5-final-fmt.log`, and `issue-1119-r5-final-lint.log`.
- Exact candidate, contract, base, and working-tree identity validation passed (`evidence/issue-1119-proof-final-validation.md`).

## Unresolved gaps

- None for R1–R8. The broader E2E and upgrade logs were not rerun on the final snapshot; their relevant code and assertions are unchanged. The final R5 retry assertions were run on the final candidate.

## Repairs needed

- None.

## Next steps

1. The matching full review and proof now establish acceptance evidence for this exact candidate.
2. Since publication is requested and no pull request exists, prepare the exact draft preview with `/publish-pr .p2p/work/issue-1119-immediate-downstream-consistency/candidate.json; review .p2p/work/issue-1119-immediate-downstream-consistency/review.md; proof .p2p/work/issue-1119-immediate-downstream-consistency/proof.md; target main; draft only`.
