# Acceptance contract: #1119

Contract revision: v1
Source: [Imported #1119 issue text](sources/issue-1119.md), [repository constraints](../AGENTS.md)
Source attribution: [trickle-labs/pg-trickle#1119](https://github.com/trickle-labs/pg-trickle/issues/1119), updated 2026-09-26T20:18:39Z, retrieved 2026-09-26T20:23:39Z. The retained source includes the complete issue body and retrieval command. No comments or amendments were present.
Parent: None
Prerequisites: The cascading IMMEDIATE support from PR #1118 is present in the inspected checkout at `20c1476b6a8ad4a57cfc5441069f333afc110511`. Required mechanisms include IVM triggers on stream-table sources, downstream stream-table buffers, and the existing repair API. Confirm these remain available at implementation handoff.

Intended outcome: Full refresh and IMMEDIATE truncate-and-repopulate preserve downstream results. IMMEDIATE descendants are correct within the originating transaction. Deferred descendants receive complete committed changes through differential refresh. Operators can recover pre-existing broken cascades, and upgraded installations receive the changed quick_health view.

Advisory learnings: None. `docs/retrospective-learnings.md` is absent.

## Evidence context

The issue records user-approved scope. This initial contract normalizes that scope and its necessary invariants. It does not authorize implementation or publication. #1116 and PR #1118 are background and prerequisite context, not a parent contract or a decomposition. No existing #1119 contract, pending amendment, or legacy proof was found.

Source keys S1-S3, E1-E2, I1, and X1 refer to the retained [issue text](sources/issue-1119.md#planning-source-keys). Repository observations below identify evidence paths, not acceptance results.

- Manual full refresh reaches `execute_manual_full_refresh_target` in [refresh_ops.rs](../src/api/refresh_ops.rs). Scheduled full refresh reaches `execute_full_refresh` in [refresh/merge/mod.rs](../src/refresh/merge/mod.rs). Both currently use blanket `DISABLE TRIGGER USER` and `ENABLE TRIGGER USER`.
- [ivm.rs](../src/ivm.rs) handles source TRUNCATE through `pgt_ivm_handle_truncate`. Existing full-refresh snapshot and diff capture functions in `src/refresh/` already support downstream buffers. Reuse these maintenance and capture mechanisms where applicable.
- Reuse [E2eDb](../tests/e2e/mod.rs), the exact result comparator in [oracle.rs](../tests/e2e/oracle.rs), [IVM tests](../tests/e2e_ivm_tests.rs), [user-trigger tests](../tests/e2e_user_trigger_tests.rs), [repair tests](../tests/e2e_repair_tests.rs), and [upgrade tests](../tests/e2e_upgrade_tests.rs). Proposed case names below are evidence to add, not claims that cases already exist.
- Full result comparison means all user columns, schema, values, NULLs, and duplicate multiplicity. Evaluate the defining query in the same database snapshot as the observation. For stale-cascade recovery, also expand the query to base tables so stale upstream results cannot validate stale descendants.
- Same-transaction checks must use one acquired connection, a SQL transaction, and assertions before COMMIT. A helper that acquires a separate pooled connection cannot establish pre-commit visibility. Disable background scheduling in manual cases. Scheduled cases must witness the actual scheduled FULL completion without a manual downstream refresh masking it.

## Acceptance matrix

| ID | Source | Requirement | Boundaries / counterexamples | Seam | Oracle | Planned evidence | Plan state |
|---|---|---|---|---|---|---|---|
| R1 | S1, E1 | Full refresh preserves synchronous maintenance of active IMMEDIATE descendants when application triggers are suppressed. | An application trigger exists on the refreshed upstream. Replacement deletes old rows and inserts changed rows, including an empty replacement. Manual and scheduled FULL paths must work. Manually refreshing the descendant cannot satisfy this row. | `pgtrickle.refresh_stream_table`, scheduled FULL refresh, descendant SELECT | Descendant defining query evaluated against the replacement upstream, with literal fixture values to witness changed content | Add `test_ivm_full_refresh_preserves_immediate_descendants` and `test_ivm_scheduled_full_refresh_preserves_immediate_descendants` in `tests/e2e_ivm_tests.rs`. Include a second IMMEDIATE level. For manual refresh, compare both descendants before COMMIT and after reconnect. For scheduled refresh, witness its completed FULL history entry and compare both descendants at a stable checkpoint. | planned |
| R2 | S1, existing suppression behavior required by E1 | FULL refresh continues to suppress application-trigger side effects under the existing suppression configuration. | Fixing R1 by enabling every trigger lets replacement writes invoke application audit or mutation logic. Empty replacement must not bypass suppression. | Full-refresh API and application-trigger audit table | Expected zero application-trigger invocations during suppressed FULL replacement | Extend `tests/e2e_user_trigger_tests.rs::test_full_refresh_suppresses_triggers` with an IMMEDIATE child and INSERT/TRUNCATE audit triggers. Require zero audit writes and exact descendant contents. Cover scheduled FULL through the R1 scheduled case. Keep existing notification and configuration behavior unchanged. | planned |
| R3 | S1 | Temporary suppression preserves every affected trigger's prior enabled state and leaves maintenance triggers operational. | A previously disabled application trigger becomes enabled, or an ALWAYS/REPLICA trigger becomes ordinary. A maintenance trigger changes state because it shares the relation. | Full-refresh API and `pg_catalog.pg_trigger` | Literal pre-operation mapping of trigger identity to `tgenabled`, plus R1 behavior | Add `test_user_triggers_full_refresh_preserves_enabled_states` in `tests/e2e_user_trigger_tests.rs`. Seed application triggers in ordinary, disabled, replica, and always states alongside valid IVM triggers. Compare the complete trigger-state mapping after successful manual and scheduled FULL refresh. R5 covers restoration after failure. | planned |
| R4 | S2, E2, I1 | IMMEDIATE truncate-and-repopulate captures the complete changes needed by deferred consumers. | Base to IMMEDIATE to DIFFERENTIAL retains removed rows; empty input, repeated empty truncation, duplicate or NULL payloads, or a query that still emits a row after truncation. Two consumers must each receive the changes even if one refreshes later. | Base `TRUNCATE`, optional committed repopulation, then refresh of each DIFFERENTIAL child | Exact defining-query results and known committed fixture checkpoints | Add `test_ivm_truncate_captures_deferred_changes` in `tests/e2e_ivm_tests.rs`. Seed and initialize the chain, commit base TRUNCATE, and refresh only the deferred child. Compare exact results. Include a scalar aggregate that emits a zero-count row, pending committed DML before truncation, a later insert batch, and repeated child refresh with no new writes. In a two-consumer variant, refresh the consumers separately and require both to converge without duplicates or lost changes. | planned |
| R5 | I1, repository durability constraint | Replacement maintenance and downstream capture commit or roll back atomically with the originating operation. | Failed repopulation or buffer capture leaves partial replacement rows, altered trigger states, advanced frontiers, or phantom downstream changes. A later retry loses an earlier committed batch. | SQL transactions around full refresh and base TRUNCATE, then descendant refresh | Pre-operation committed row sets, buffer contents, trigger-state mapping, and consumer frontiers | Add `test_ivm_replacement_failure_rolls_back_and_recovers` in `tests/e2e_ivm_tests.rs`. Cover both replacement paths with explicit rollback and a deterministic SQL error after replacement begins, such as a downstream CHECK rejection or test-owned failing buffer INSERT trigger. Verify that the intended error occurred, roll back, and compare retained state from another connection. Remove the fault, retry, commit, reconnect, and require exact IMMEDIATE and deferred results including pending committed changes. Ignore sequence gaps, which are not lost change records. | planned |
| R6 | I1, S2 | An eligible DIFFERENTIAL descendant consumes captured replacement changes without a forced FULL rebuild used to hide missing capture. | The child's contents become correct only because its refresh silently falls back to FULL, or a second no-op refresh applies the batch again. | Child refresh API and refresh history/effective-mode diagnostics | DIFFERENTIAL execution for a small eligible fixture, plus R4 exact query oracle | In R4's cases, configure the existing adaptive thresholds to keep the fixture eligible for differential maintenance. Assert the child's recorded execution mode is DIFFERENTIAL and verify exact output after the first and repeated refresh. Full replacement of the directly truncated IMMEDIATE table remains allowed. | planned |
| R7 | S3, pre-existing broken cascades | Published recovery guidance gives operators a working procedure to repair pre-existing broken cascades and restore their results. | Installing the fixed library alone is described as repairing stale data or missing triggers. Repairing only the lowest descendant leaves an upstream stale. Existing same-version installations cannot follow an `ALTER EXTENSION UPDATE` instruction to reapply that version's migration. | `docs/UPGRADING.md`, public diagnostics and repair/refresh APIs | The documented SQL, exact base-derived results, and a subsequent synchronously maintained write | Add `test_ivm_documented_cascade_recovery_restores_results` in `tests/e2e_repair_tests.rs`. Seed a multi-level stale cascade with missing or disabled IVM triggers, follow the documented procedure in dependency order, and compare each level against base-derived expected results. Perform another write and check descendants before COMMIT. Guidance must distinguish installing code, applying the applicable migration, and repairing old state; include a supported procedure for already-installed affected versions. | planned |
| R8 | S3 | The release migration installs the changed `pgtrickle.quick_health` view on existing installations. | Fresh installation exposes `broken_immediate_tables` but upgrade does not; missing or disabled IVM triggers fail to affect the upgraded view. Existing columns or grants disappear. | Real `ALTER EXTENSION pg_trickle UPDATE` and `SELECT` from `pgtrickle.quick_health` | Documented view columns/types and literal health outcomes: healthy cascade has zero broken tables, missing/disabled maintenance reports a broken table and CRITICAL, repaired cascade returns to zero | Add `test_upgrade_quick_health_reports_broken_immediate_cascades` in `tests/e2e_upgrade_tests.rs`. Upgrade an old-version container with existing streams using the candidate migration. Compare upgraded view shape and definition with a fresh candidate installation; retain access grants. Exercise healthy, missing-trigger, disabled-trigger, and repaired states. At this source revision the migration is `sql/pg_trickle--0.107.0--0.108.0.sql`; use the actual release successor if the release target changes and record the evidence-path update. | planned |

## Planned verification commands

These commands are for implementation and proof. No behavioral verification runs during this planning stage.

```sh
just fmt
just lint
just test-unit
just build-e2e-image
./scripts/run_e2e_tests.sh --test e2e_ivm_tests --test e2e_user_trigger_tests --test e2e_repair_tests --test e2e_phase4_ergonomics_tests
just check-upgrade 0.107.0 0.108.0
just test-upgrade 0.107.0 0.108.0
```

Use Testcontainers and the existing PostgreSQL 18 extension images, never a local PostgreSQL server. The full runner is deliberate because scheduled behavior and a real upgrade exceed the light E2E path. All named new cases must execute. Skipped or unavailable upgrade infrastructure cannot establish R8. Static migration completeness alone cannot establish changed-view behavior. Retain the actual selected tests and results in the later proof report. Adjust version arguments together with the actual release target, without changing R8's promise.

## Unresolved gaps

- None. Each row has an existing public seam, independent oracle, and concrete evidence plan. The new regression cases and assertions still need implementation.

## Open questions

- None within the issue's approved scope. The current release migration is the planning baseline; a later release target changes the migration location, not the required upgrade behavior.

## Out of scope

- Broader redesign, ticket decomposition, a new maintenance or capture framework, and new public configuration.
- Automatic repair of all existing installations merely by loading a new library. R7 requires explicit, tested recovery guidance.
- Exhaustive crash, failover, query-shape, or performance qualification unrelated to these replacement paths. Existing repository constraints and relevant regression checks remain binding.

## Change notes

- Initial contract, v1. Allocated R1-R8. There are no prior requirement IDs to migrate and no checked source acceptance boxes or historical proof verdicts.
- S1 maps to R1-R3, S2 to R4 and R6, S3 to R7-R8, E1 to R1-R2, E2 to R4, and I1 to R4-R6 and the reuse constraint. X1 maps to the exclusions. R2 preserves the application-trigger suppression explicitly assumed by S1. R5 makes the issue's atomicity and committed-change promise observable. Scheduled coverage follows the same full-refresh promise through the second existing execution path.
- Source inspection is not reproduction or proof. No product files, tests, tracker labels, or issue bodies were changed by this planning stage.

## Implementation handoff

Hand off to an explicitly authorized `/implement-contract work/issue-1119-immediate-downstream-consistency.md` invocation.
Implement the smallest complete solution inside the spec envelope.
Preserve requirement IDs and promised outcomes.
Capture the resulting candidate for separate `/review-implementation` and `/prove` phases.

## Proof handoff

Evaluate every requirement against this contract revision and one fixed candidate.
Record actual evidence and verdicts in `.p2p/work/issue-1119-immediate-downstream-consistency/proof.md`, separate from this contract.
Retain the exact candidate and contract identities and the observations needed to interpret each named assertion.
