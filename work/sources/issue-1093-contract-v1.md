<!-- acceptance-contract:1093:v1 -->
# Acceptance contract: #1093

Contract revision: v1
Source: [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), What to build and acceptance criteria; [#1090](https://github.com/trickle-labs/pg-trickle/issues/1090), working agreement and completion
Parent contract: [#1090](https://github.com/trickle-labs/pg-trickle/issues/1090) v1
Parent snapshot: #1090 acceptance-contract:1090:v1 block in the canonical issue body; SHA-256: c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982
Contribution: Refines #1090 v1:Q1090-R01, Q1090-R02, Q1090-R03, Q1090-R04, and Q1090-R05 for release evidence and publication qualification; owns required-case and candidate-package qualification.
Prerequisites: None to start. The #1091 result contract is proven in merged PR #1100 and #1092 is PROVEN in its issue report; later instrumented results from #1098 are registered when available.

Intended outcome: Make CI and release qualification reject missing required cases and a package different from the release candidate. Extend the existing release-suite runner, evidence writer, workflows, and qualification record. Start with explicit #1091/#1092 case identities, then register each later slice as it lands. Account for selected and observed cases, shards, skips, and retries. Install the candidate package into fresh PostgreSQL processes and verify installed extension files against it. Distinguish exact-release runs from instrumented builds.

## Evidence context

Extend [run_release_suite.py](https://github.com/trickle-labs/pg-trickle/blob/main/scripts/run_release_suite.py),
[release_evidence.py](https://github.com/trickle-labs/pg-trickle/blob/main/scripts/release_evidence.py),
[the current release gate](https://github.com/trickle-labs/pg-trickle/blob/main/scripts/v0_108_0_release_gate.py), and its existing
[shared negative controls](https://github.com/trickle-labs/pg-trickle/blob/main/scripts/v0_106_1_release_gate.py).
Use [v0.108 qualification](https://github.com/trickle-labs/pg-trickle/blob/main/tests/release/v0.108.0-qualification.json) or its
successor, [CI](https://github.com/trickle-labs/pg-trickle/blob/main/.github/workflows/ci.yml), and
[release](https://github.com/trickle-labs/pg-trickle/blob/main/.github/workflows/release.yml). Proposed `check_*` names below are
checks to add at these entry points, not a separate reporting system.

The first required identities are exactly:

```text
e2e_sensitivity_baseline_tests::test_oracle_detects_same_count_different_content
e2e_sensitivity_baseline_tests::test_oracle_detects_duplicate_multiplicity_mismatch
e2e_sensitivity_baseline_tests::test_oracle_detects_schema_mismatch
e2e_sensitivity_baseline_tests::test_oracle_detects_missing_user_column
e2e_failure_recovery_tests::test_lock_timeout_during_refresh
e2e_failure_recovery_tests::test_statement_timeout_during_refresh_recovers
e2e_failure_recovery_tests::test_cancel_backend_during_refresh_recovers
e2e_dvm_failpoint_tests::test_dvm_failpoint_preserves_last_committed_result_and_recovers
```

## Acceptance matrix

| ID | Source | Requirement | Boundaries / counterexamples | Seam | Oracle | Planned evidence | Plan state |
|---|---|---|---|---|---|---|---|
| Q1093-R01 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC1 | A complete valid candidate run passes validation. | A stricter validator rejects an otherwise valid run. | Release-suite runner and evidence CLI | The declared eight-case contract and real runner observations | `check_complete_candidate_run_accepted`. Feed a real runner-produced result set with all required identities, installation observations, attempts, and logs to `release_evidence.py`; require exit 0. Synthetic records can test parsing but cannot establish candidate execution. | planned |
| Q1093-R02 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), W/AC1 | Omitted required cases, empty selections, and missing case outcomes are rejected. | A different passing test replaces the required case while total count stays positive. | Release-suite runner and evidence CLI | The eight fully qualified identities in this ticket | `check_required_case_omissions_rejected`. Remove each of the eight identities in turn, use an empty selector, and remove one observed result. Invoke the runner/evidence CLI and require nonzero exit naming the omitted identity or empty selection. | planned |
| Q1093-R03 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC1 | Required shard coverage is complete and unambiguous. | One missing shard or one duplicate shard appears complete by aggregate totals. | Release evidence CLI | Declared shard IDs and disjoint required-case allocation | `check_required_shards_rejected`. Start from an explicit two-shard baseline with disjoint required identities. Delete a shard, duplicate its ID, or assign the same required case twice while omitting another; require the corresponding shard/case diagnostic. | planned |
| Q1093-R04 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC1/AC5 | An unexpected skip or unavailable required infrastructure cannot qualify a case. | Required case is ignored, Docker is absent, setup fails, or a skip is counted as execution. | Release-suite runner and evidence CLI | Required cases must have observed execution; unavailable infrastructure has no result | `check_required_nonexecution_rejected`. Exercise each result state through the runner/validator. Require non-passing aggregate evidence even when unrelated cases pass. Preserve setup diagnostics. | planned |
| Q1093-R05 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC4 | A retry cannot erase an earlier required correctness failure. | Attempt 1 fails and attempt 2 passes; only attempt 2 survives. | Release evidence CLI | All ordered attempts, including the first correctness failure | `check_correctness_retry_retains_failure`. Supply both ordered attempt records, then try a record that drops the first. Require non-passing qualification and retained first-attempt diagnostics. Inspect `.config/nextest.toml` retry behavior when collecting results. | planned |
| Q1093-R06 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), W/AC2/AC3 | Runtime tests execute extension files installed from the declared candidate in fresh PostgreSQL processes. | Correct archive hash but stale mounted `.so`, control file, or SQL file; preloaded old library survives a file replacement. | Candidate package installer and release evidence CLI | Extracted candidate manifest and fresh postmaster identity | `check_installed_candidate_payload`. Install the candidate before starting each test server. Hash installed library/control/SQL files inside that container and compare with the extracted candidate manifest. Record fresh postmaster identity and platform. A stale-image or prestarted-server control must be rejected. | planned |
| Q1093-R07 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC2 | Running package A while declaring candidate B fails even when all selected tests pass. | Metadata declares B while the server actually loads A. | Candidate package installer and release evidence CLI | Candidate B manifest versus installed package A file digests | `check_wrong_candidate_package_rejected`. Use two distinguishable, loadable packages. Deliberately install A and declare B through the real runner. Require installed-payload mismatch, with otherwise passing test outcomes retained. Merely changing a JSON digest is insufficient. | planned |
| Q1093-R08 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC1/AC3 | Candidate commit, artifact, platform, and advertised feature scope agree across results. | Correct results attached to another commit, architecture, feature configuration, or suite version. | Release evidence CLI | Declared commit, platform, artifact and feature scope | `check_candidate_identity_mismatch_rejected`, extending existing negative controls. Alter one identity at a time and require a field-specific rejection. Runtime-qualified claims need runtime evidence for that scope; build-only artifacts remain labeled build-only. | planned |
| Q1093-R09 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC3 | CI retains enough evidence to reproduce and diagnose every required run. | Missing command, server/configuration, case identities/outcomes, package/installed-file digest, seed, or raw diagnostic. | Release evidence CLI and retained CI artifacts | Runner observations and referenced raw logs | `check_required_evidence_fields`. Validate runner-produced commit, actual argv, server version and durability settings, artifact and installed-file hashes, selection/observations/attempts, configuration, relevant seeds, and referenced nonempty logs. Missing or tampered required evidence fails. | planned |
| Q1093-R10 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC3 | A self-declared pass cannot replace observed execution. | `--suite name=passed`, a hand-authored summary, or a positive total without required case events. | Release evidence CLI | Observed case events, not a reported total | `check_unobserved_pass_rejected`. Pass each unsupported form to the publication evidence path and require rejection for missing observed evidence. Extend existing CLI negative controls rather than adding a parallel evidence writer. | planned |
| Q1093-R11 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), W; [#1098](https://github.com/trickle-labs/pg-trickle/issues/1098), AC4 | Instrumented results cannot satisfy an exact-release runtime requirement. | ASan library digest is presented as the unmodified shipped library. | Release evidence CLI | Shipped-library digest and build kind | `check_instrumented_evidence_cannot_replace_candidate`. Accept separate identified instrumented evidence, then substitute it for a required candidate runtime result and require a build-kind/payload rejection. | planned |
| Q1093-R12 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC4 | Publication is gated by validated candidate evidence. | GitHub release, Docker, GHCR, or PGXN promotion proceeds after qualification fails or is skipped. | Release workflow publication jobs | Validated evidence result and publisher dependency graph | extend the existing workflow-dependency checks in the shared/current release gates. `check_quality_publication_dependencies` checks direct and downstream publishers, including workflow-run success conditions, and rejects a fixture with one qualification dependency removed. No actual publication is needed to test the guard. | planned |
| Q1093-R13 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC4 | Release evidence and its referenced diagnostics remain attached to the release artifacts. | Validation passes locally but publishing omits evidence or its raw files. | Release asset staging and publication workflow | Validated evidence manifest and staged asset digests | `check_quality_evidence_packaging`. Exercise the existing evidence packaging step into a local staging directory and require the validated evidence plus all referenced logs/measurements with matching hashes. Workflow checks require those staged assets in publication. | planned |
| Q1093-R14 | [#1093](https://github.com/trickle-labs/pg-trickle/issues/1093), AC5 | Existing upgrade, platform, security, compatibility, and performance gates remain required. | New quality suites replace or bypass an existing mandatory gate. | Release and PR CI workflow gates | Pre-change mandatory gate inventory | extend current release/CI contract checks against the pre-change required-suite and job inventory. `check_quality_preserves_existing_gates` rejects deletion or bypass of any existing mandatory gate; adding a child slice does not loosen them. | planned |

## Unresolved gaps

- None. New controls are planned; no row claims proof.

## Open questions

- None.

## Out of scope

- Broad routing, ledgers, dashboards, and measurement pipelines.

## Change notes

- Initial contract, v1. Existing matrix IDs Q1093-R01–Q1093-R14 retain their meanings; GitHub issue history preserves the prior matrix. The original acceptance checkboxes remain source criteria, not proof.

## Storage handoff

Canonical destination: this issue body, revision v1. Reread this saved issue before use.

## Implementation handoff

Implement the smallest complete solution inside the spec envelope. Preserve requirement IDs and promised outcomes.

## Proof handoff

Evaluate every requirement against this contract revision and one fixed candidate. Record actual evidence and verdicts in a separate proof report.
<!-- /acceptance-contract:1093:v1 -->
