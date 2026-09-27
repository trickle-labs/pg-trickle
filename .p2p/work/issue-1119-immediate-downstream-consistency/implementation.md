# IMPLEMENTED: issue #1119

Contract: `work/issue-1119-immediate-downstream-consistency.md` v1; SHA-256 `c7ecbeb273d2fd8ed8ef6ef3cc31fd0f2128550049a1daea93896bf7626592b8`
Scope: R1-R8
Comparison base: `2dfe8dae90452120cf6760b71dd0def8c241756b`
Candidate: `snapshot:sha256:08f36577aca0522020d99d2bad996917b98746f08d0875bb06fb3596176405c9`

FULL refresh suppresses application triggers while keeping IVM maintenance active, and the trigger detector excludes only actual IVM maintenance functions. Incremental downstream capture snapshots rows matching the materialized delta, then pairs occurrences within each row identity so duplicate additions and removals produce the corresponding buffer events. FULL capture uses the same multiset comparison across the complete pre/post snapshots.

The review findings about keyless duplicate counts and reserved-looking application trigger names were fixed. R5 rollback evidence was strengthened: before both explicit rollback and injected-failure paths, the test saves exact base, stream, and buffer rows, all relevant frontiers, and trigger modes. After rollback or failure, it compares that complete state through a newly opened connection, retries the operation, and compares the exact immediate and deferred results from another fresh connection.

## Requirement handoff

| ID | Implementation and evidence | Result |
|---|---|---|
| R1 | Manual and scheduled FULL tests verify two IMMEDIATE levels without manually refreshing descendants. | Implemented; focused E2E suite passed. |
| R2 | Audit triggers remain quiet during manual and scheduled FULL, including when application trigger names resemble reserved prefixes. | Implemented; focused E2E suite passed. |
| R3 | Trigger enabled-state mappings are preserved across successful and failed refreshes while IVM triggers remain active. | Implemented; focused E2E suite passed. |
| R4 | TRUNCATE, duplicate-count changes, NULL values, empty replacements, repeated truncation, zero-count aggregates, later inserts, separate deferred consumers, and FULL replacement capture are checked against query results. | Implemented; focused E2E suite passed. |
| R5 | Explicit rollbacks and injected failures compare exact table/buffer rows, all affected frontiers, and trigger states from a fresh connection; CHECK failures and retries are verified against defining queries after reconnect. | Implemented; focused regression passed. |
| R6 | The first deferred refresh consuming TRUNCATE capture records `DIFFERENTIAL` with no full fallback, and repeated refreshes match the query. | Implemented; focused E2E suite passed. |
| R7 | Documented repair runs in dependency order; a follow-up write is checked against base-derived results before COMMIT and after reconnect. | Implemented; focused E2E suite passed. |
| R8 | The real 0.108.0→0.108.1 extension upgrade verifies `quick_health`, grants, and healthy/broken/repaired cascade states. | Implemented; 33 upgrade tests passed. |

## Checks

- `just fmt` passed (`evidence/issue-1119-r5-final-fmt.log`); `just lint` passed with zero warnings (`evidence/issue-1119-r5-final-lint.log`).
- `just test-unit`: 2,587 passed with host loopback access (`issue-1119-repair-final2-unit.log`).
- `just test-integration`: 161 passed, including trigger-detection integration cases (`issue-1119-repair-final-integration.log`).
- PostgreSQL 18 E2E image rebuilt from the Rust implementation (`issue-1119-repair6-e2e-image.log`).
- Focused IVM, user-trigger, repair, and phase-4 suites: 92 passed, 0 skipped (`issue-1119-repair-final4-related-e2e.log`). The final R5 fresh-connection and retry-persistence regression passed 1/1 (`evidence/issue-1119-r5-final-e2e.log`).
- `just test-upgrade 0.108.0 0.108.1`: 33 passed, including the quick-health migration case (`issue-1119-repair-final2-upgrade.log`).
- Version synchronization, upgrade completeness, and `git diff --check`: passed.

The contract did not change. Matching `REVIEWED` and `PROVEN` reports for this exact candidate are saved in `review.md` and `proof.md`. No commit, push, tracker update, or PR update was made.
