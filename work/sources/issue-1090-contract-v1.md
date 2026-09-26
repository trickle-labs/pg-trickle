<!-- acceptance-contract:1090:v1 -->
# Acceptance contract: #1090

Contract revision: v1
Source: [#1090](https://github.com/trickle-labs/pg-trickle/issues/1090), Goal, First milestone, Completion, Working agreement, Deferred scope, Historical plans, and the legacy acceptance matrix
Parent contract: None

Intended outcome: Make wrong results, lost committed changes, broken recovery, and misleading release claims harder to ship through executable protection in the relevant test and release workflows. Deliver the eight bounded slices, preserve existing regression evidence, and formally verify one small production implementation without claiming whole-extension verification.

## Acceptance matrix

| ID | Source | Requirement | Boundaries / counterexamples | Seam | Oracle | Planned evidence | Plan state |
|---|---|---|---|---|---|---|---|
| Q1090-R01 | #1090 Working agreement | Each new gate accepts a correct baseline and rejects a representative semantic defect for the intended reason. | A control fails to compile, misses its target, or fails only because infrastructure is absent; unrelated failures do not count. | The child test or CI gate and its retained result | Correct baseline fixture plus the named semantic defect | Each bounded child slice records a passing baseline, targeted negative control, rejected assertion, and retained logs in its existing test or CI result. | planned |
| Q1090-R02 | #1090 First milestone | The first milestone has passing #1091 and #1092 CI results, #1093 omission/package rejection, and the immediate correction of misleading qualification wording. | Compilation or aggregate suite totals are reported as completion without the required cases and controls. | Existing CI and release qualification records | The eight required case identities, Q1093-R02/R07 controls, and Q1096-R01 wording review | Existing CI and release results include the named cases, required controls, and wording correction before the milestone is called complete. | planned |
| Q1090-R03 | #1090 Completion | Every delivered bounded slice runs in its relevant CI and release job before the epic is called complete. | A test exists but is not selected, an issue closes with an unmet row, or optional deferred work becomes an implicit gate. | Child issue checklists, selected test identities, and CI/release records | Each child's acceptance matrix and observed run results | Account for every child requirement and selected case in existing CI/release records; optional deferred work adds no gate. | planned |
| Q1090-R04 | #1090 Deferred scope and historical plans | Existing regression corpora and historical research remain available without being represented as completed new work. | A regression is deleted or dropped, or a closed historical plan is treated as proof of implementation. | tests/corpus/dvm_regressions, CI selection, candidate diff, and historical links | Repository inventory and the source issue history | Check the corpus inventory, CI selection, and candidate diff; require an explicit authorized change for deletion or dropped selection. | planned |
| Q1090-R05 | #1090 Goal and working agreement | Public completion claims stay within the behaviors and artifacts actually qualified. | The extension is claimed verified from an LSN proof, unsupported platform claims, or an exposed defect hidden by a weaker assertion. | Release claim and qualification evidence | The issue checklist and retained candidate evidence | Review each claim against qualified behaviors and artifacts; exposed defects remain visible until fixed or an explicit supported-contract restriction is recorded. | planned |
| Q1090-R06 | #1090 Goal; #1097 formal-verification contribution | At least one small production implementation used by the extension is formally verified and integrated into its named callers, with public claims limited to the checked operations. | A separate model is proved but production callers do not use it, the proof can be skipped, or the claim expands to whole-extension correctness. | #1097's checked LSN implementation, caller inventory, proof job, and candidate qualification | Named proof obligations plus runtime caller and package-binding evidence | Use #1097's checked LSN proof, semantic and skip controls, caller integration, and #1093 candidate-binding evidence. The pinned grammar and Verus path are contract inputs; implementation must create and run them. | planned |

## Unresolved gaps

- None. The parent contract now has explicit grammar and verifier inputs for the bounded formal-verification slice.

## Open questions

- None.

## Out of scope

- General mutation platforms, expanded coverage or semantic dashboards, exhaustive concurrency and failover matrices, proof families beyond checked LSN operations, a separate verification release system, and any unsupported release claim.
- Historical plans and deferred scope are context, not completed work.

## Change notes

- Q1090-R06 planning inputs are resolved through #1097's explicit PostgreSQL 18 grammar and pinned Verus path; requirement meaning is unchanged.

- Initial contract, v1. Existing Q1090-R01–Q1090-R05 promises are reconciled from the legacy matrix and retain their meanings.
- Q1090-R06 is added from the Goal's explicit formal-verification promise and maps to #1097's bounded LSN contribution.

## Storage handoff

Canonical destination: this issue body, revision v1. Reread this saved issue before use.

## Implementation handoff

Implement the smallest complete solution inside the spec envelope. Preserve requirement IDs and promised outcomes. Child work remains independently bounded by its own contract and this parent contract.

## Proof handoff

Evaluate every requirement against this contract revision and one fixed integrated candidate. Record actual evidence in a separate proof report.
<!-- /acceptance-contract:1090:v1 -->
