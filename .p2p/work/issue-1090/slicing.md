# #1090 slice coverage and approved delivery plan

Plan revision: v1  
Parent: [canonical #1090 work item](../../../work/issue-1090.md), acceptance contract v1  
Parent issue: https://github.com/trickle-labs/pg-trickle/issues/1090  
Parent issue snapshot: [work/sources/issue-1090.json](../../../work/sources/issue-1090.json), SHA-256 `07a0d8f3d0992dfc13af90ebcc01d179c4673df2a2337d852db276feac3667e5`  
Parent contract snapshot: [work/sources/issue-1090-contract-v1.md](../../../work/sources/issue-1090-contract-v1.md), SHA-256 `0ad7a2c5dae2acc49bf3652c70d2ee5780685f92caa33271a6ccf176fcc7b8d8`  
Current child status snapshot: [work/sources/issue-1090-child-statuses-2026-09-27.json](../../../work/sources/issue-1090-child-statuses-2026-09-27.json), SHA-256 `1e6b447b0ea950bd78eaf81c0c07ca1011b02ca1651b7dc84805822618ef5cda`  
Repository: `trickle-labs/pg-trickle`  
Target base observed from GitHub: `main` at `2dfe8dae90452120cf6760b71dd0def8c241756b`.

## Source and scope

The parent agreement is the unchanged v1 contract copied from the saved source snapshot. The source issue lists eight bounded child issues and says to ship improvements incrementally. The issue bodies and acceptance matrices remain in their linked source records. This plan allocates each child to the parent promises and records the user's approved delivery route; it does not change child acceptance criteria.

## Coverage map

| Parent promise | Contributing child or parent check | Contribution and completion check |
|---|---|---|
| Q1090-R01 — each gate accepts a correct baseline and rejects a representative semantic defect | #1091–#1098 | Each child owns its named baseline and negative control. Check the child source matrix and retained run evidence for the intended failure reason. |
| Q1090-R02 — complete the first milestone | #1091, #1092, #1093, #1096 | #1091/#1092 supply passing named CI cases; #1093 rejects omissions and wrong packages; #1096 corrects and qualifies publication wording. Check all four contributions before calling the milestone complete. |
| Q1090-R03 — each bounded slice runs in relevant CI and release qualification | #1091–#1098, with #1093 as the package-evidence seam | Each child supplies relevant selected case identities; #1093 binds release evidence to the tested candidate. Parent completion checks every child outcome on the integrated candidate. |
| Q1090-R04 — preserve regression corpora and historical research without claiming deferred work | #1091, #1093, #1095, plus parent completion audit | #1091 retains representative regression checks; #1093 retains source and qualification provenance; #1095 preserves replay/cleanup correctness. The final parent audit checks `tests/corpus/dvm_regressions`, its CI selection, historical links, and candidate diff. |
| Q1090-R05 — limit public completion claims to qualified behavior and artifacts | #1091–#1098, with #1093 binding evidence | Each child limits its claims to its cases and supported environment. #1093 binds evidence to the candidate. Parent proof reviews the final issue/release wording against that evidence. |
| Q1090-R06 — formally verify and integrate one production LSN implementation | #1097, with #1093 candidate binding | #1097 proves the named checked operations and integrates listed callers; #1093 binds proof and runtime evidence to the candidate package. Parent proof confirms actual caller and artifact identity. |

## Child outcomes and direct prerequisites

| Child | Outcome | Direct prerequisite for acceptance |
|---|---|---|
| [#1091](../../../work/issue-1090-public-result-schema.md) | Exact independent public-result and schema comparisons with semantic controls. | None. |
| [#1092](../../../work/issue-1090-witnessed-failures.md) | Witnessed timeout, cancellation, scheduler failure, and exact recovery. | None. |
| [#1093](../../../work/issue-1090-release-qualification.md) | Reject omitted cases and mismatched candidate packages; retain complete candidate-bound evidence. | #1091 and #1092 case identities for the first milestone. |
| [#1094](../../../work/issue-1090-refresh-atomicity.md) | Prevent partial output/progress across named failure and concurrency boundaries. | #1091 exact comparison and #1092 witnessed fault/session controls. |
| [#1095](../../../work/issue-1090-lossless-replay-cleanup.md) | Preserve committed changes through receipt, replay, acknowledgement, and safe cleanup. | #1091 exact comparison and #1092 witnessed interruption controls. |
| [#1096](../../../work/issue-1096-real-publisher-to-subscriber-recovery.md) | Prove exact committed delivery to a real subscriber through interruption and restart. | #1091 comparator, #1092 interruption controls, and #1093 candidate evidence. |
| [#1097](../../../work/issue-1090-production-lsn.md) | Verify one checked LSN implementation and integrate it into named production callers. | Implementation can start immediately; final candidate qualification uses #1091 and #1093. |
| [#1098](../../../work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers.md) | Run selected real unsafe-boundary workloads with assertions and ASan, separately from the release package. | Implementation/local instrumentation can start immediately; final qualification uses #1092 and #1093. |

The #1091, #1092, and #1093 prerequisites needed by the open #1097/#1098 final qualification are recorded as closed in the current child-status snapshot. Child issue closure is not a substitute for checking the referenced evidence.

## Dependency graph and stable sequence

Acceptance dependencies are #1091 and #1092 before final acceptance of #1093, #1094, and #1095; #1091, #1092, and #1093 before #1096; and #1091/#1093 before final candidate qualification of #1097. #1098's final release qualification uses #1092/#1093. These dependencies are satisfied for the open children; there is no integration-branch prerequisite.

Stable topological implementation sequence by child number: #1091, #1092, #1093, #1094, #1095, #1096, #1097, #1098. This is an execution order; it adds no new dependency beyond the acceptance prerequisites above.

## Parent completion plan

After every child is accepted and delivered to `main`, assess Q1090-R01–R06 against one exact integrated main candidate. Run the relevant child suites and release qualification on that candidate, inspect the retained regression corpus and source history, and review public claims against actual evidence. Recheck the named #1097 production callers and candidate binding. Child proofs remain evidence for their own scope and are not combined into a parent verdict. No separate integration-code ticket is needed; the parent-level integrated-candidate assessment owns the remaining checks.

## Approved delivery plan

Plan revision: v1.  
Approval source: user reply in this conversation, recorded verbatim in [routing-approval-2026-09-27.md](invocations/routing-approval-2026-09-27.md), SHA-256 `e407062b7dae5e69bb2ce2dd2ae4ae5cbcbf1f1bbef24dc218ad58c89c0994b2`. The user approved “Yes, use that route” in response to routing every remaining child independently to `main`; #1098's earlier independent-to-main approval is retained separately in its child work directory.  
Final destination: main
Integration branch: none
Integration start: none
Default choice: independent
Starting target base observed on 2026-09-27: `2dfe8dae90452120cf6760b71dd0def8c241756b`. Refresh the base when preparing each future child candidate.

| Child | Choice | Destination | Reason | State |
|---|---|---|---|---|
| work/issue-1090-public-result-schema.md | independent | main | Exact result checks can ship independently. | landed |
| work/issue-1090-witnessed-failures.md | independent | main | Failure witnesses can ship independently. | landed |
| work/issue-1090-release-qualification.md | independent | main | Release evidence gates can ship independently. | landed |
| work/issue-1090-refresh-atomicity.md | independent | main | Refresh atomicity checks can ship independently. | landed |
| work/issue-1090-lossless-replay-cleanup.md | independent | main | Replay and cleanup checks can ship independently. | landed |
| work/issue-1096-real-publisher-to-subscriber-recovery.md | independent | main | Subscriber recovery can ship independently. | landed |
| work/issue-1090-production-lsn.md | independent | main | The LSN implementation can ship independently. | remaining |
| work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers.md | independent | main | The bounded unsafe-boundary slice can ship independently. | remaining |

Child exceptions: none. Each complete child outcome is acceptable on `main` if the other children never ship. No sibling payload is required in a child PR. Existing closed issues are not reopened or modified by this local plan.

Pending actions: finish #1097 and #1098 on their own candidates; update each candidate base from current `main`; verify each child independently; then perform the parent completion assessment on one exact integrated candidate. This file and its approval receipt are local records; no GitHub issue body, labels, relationships, branches, or pull requests were changed.
