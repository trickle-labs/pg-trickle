# PROVEN: issue #1096 — real publisher-to-subscriber recovery

Requirements: 9/9  
Counterexamples tested: 6 (missing row, altered value, disabled progress, missing stream registration, unreachable publisher setup, and subscriber interruption during catch-up)  
Contract: `work/issue-1096-real-publisher-to-subscriber-recovery.md` v1, SHA-256 `b7788d82e9d17a04574f298295bc8c0582adb61195505a76b370a3f9779da622`  
Contract snapshot: exact work-item bytes, SHA-256 above  
Parent context: #1090 v1 block SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`; prerequisite records #1091 and #1092; #1093 release-evidence contract and source/proof snapshot are included in the unchanged binding-input hashes  
Candidate: commit `710e657f17548fb7446e1b2384b595c3a530e162`  
Comparison base: `edf700efdbb80983fe12c0e10f84fa271b818faf`  
Candidate stability: unchanged; P2P candidate validation matches the commit and the PR still points to it  
Contract stability: unchanged  
Verification context: GitHub Actions PR run 36252436204; Ubuntu Linux, PostgreSQL 18.6, packaged Linux amd64 candidate

## Outcome

The current PR candidate satisfies all nine v1 requirements. GitHub ran the CI workflow for PR head `710e657f` against base `edf700ef`; its test package was built from merge commit `2e84d8b58f2a48b66d72479b71010f66efd66451`, whose parents are exactly that base and head. The candidate package publication suite completed on required shard `publication-topology` 3/3, with four required topology cases passed, nine tests executed, and none skipped. Its nine installation observations match candidate and installed payload file manifests exactly. Subscriber assertions compare literal expected rows; restart and catch-up evidence records the server identities, slot boundary, and observed interruption. The matching implementation review is now saved at `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/review.md`; acceptance evidence is complete for this candidate.

## Requirement verdicts

| ID | Observation and independent oracle | Evidence reference | Verdict |
|---|---|---|---|
| Q1096-R01 | The legacy case remains `test_err4_publisher_connection_recovery_smoke`; its comment says it tests publisher connection recovery only and creates/restarts no subscriber. The qualification workload now describes the real two-server topology. This repair changed only suite routing and shard metadata, not those claims. Oracle: behavior exercised by the current test bodies. | `tests/e2e_publication_crash_recovery_tests.rs` (legacy smoke and topology cases); `tests/release/v0.108.0-qualification.json` (`publication-recovery`); `roadmap/v0.26.0.md-full.md` ERR-4; four-file repair diff | proven |
| Q1096-R02 | `Pair::new` starts distinct publisher and subscriber containers, registers the actual publication, creates a real PostgreSQL subscription, waits for table readiness, and compares the subscriber with literal initial rows `(1,a,10),(2,b,20)`. The packaged topology test passed and logged `initial_sync=exact`. | `tests/e2e_publication_crash_recovery_tests.rs` (`Pair::new`, `wait_exact`); `evidence/pr1117-publication-recovery-job.log`; publication-recovery job below | proven |
| Q1096-R03 | The initial/DML test updates row 1, deletes row 2, inserts row 3, refreshes, and requires exact rows `(1,a,11),(3,a,30)`; a later batch adds `(4,a,40)`. The row oracle compares the full ordered row vector, preserving values and multiplicity. | `tests/e2e_publication_crash_recovery_tests.rs::test_publication_recovery_initial_sync_dml_and_setup_failures`; passing runtime record in `evidence/pr1117-publication-recovery.log` | proven |
| Q1096-R04 | The test stops the subscriber after initial sync, commits a publisher batch, witnesses the slot behind the target LSN, then restarts the same container and requires exact checkpoint rows and slot catch-up. The job records the outstanding LSN and changed postmaster start time. | `tests/e2e_publication_crash_recovery_tests.rs::test_publication_recovery_subscriber_restart_catches_up`; `evidence/pr1117-publication-recovery-job.log` and `evidence/pr1117-publication-recovery.log` | proven |
| Q1096-R05 | The test commits while the subscriber is down, restarts the publisher with its existing data directory, checks retained cluster/container/data identity plus the publication and logical slot, then verifies exact catch-up and a new post-restart batch. | `tests/e2e_publication_crash_recovery_tests.rs::test_publication_recovery_publisher_crash_retains_slot_and_catches_up`; passing log records `retained_identity=true` and distinct postmaster start times | proven |
| Q1096-R06 | The test blocks the logical apply worker and observes IDs `1,2,4` delivered while `50,6` remain pending, kills the subscriber during that catch-up, restarts it, and requires the exact final checkpoint and caught-up slot. | `tests/e2e_publication_crash_recovery_tests.rs::test_publication_recovery_interrupted_catchup_and_negative_controls`; `evidence/pr1117-publication-recovery-job.log` records blocked worker, pending IDs, final LSN, and exact convergence | proven |
| Q1096-R07 | The exact-row oracle rejects a missing row and altered value. Disabling the subscription leaves rows unchanged while the slot remains behind; re-enabling it converges. The bounded disabled-progress observation uses a 2-second wait. All three negative controls were detected. | Same test as R06; `evidence/pr1117-publication-recovery-job.log` records `missing_row=detected altered_value=detected disabled_progress=detected disabled_slot_backlog=true`; test source contains the corresponding assertions | proven |
| Q1096-R08 | Missing stream-table registration and an unreachable publisher each return an error; the failed subscription leaves no orphan row and cannot pass through a publisher-only fallback. The combined packaged setup-failure test passed. | `tests/e2e_publication_crash_recovery_tests.rs::test_publication_recovery_initial_sync_dml_and_setup_failures`; `evidence/pr1117-publication-recovery.log` | proven |
| Q1096-R09 | The release artifact binds the topology suite to merge candidate `2e84d8b…`, package digest `44401b1e…`, Linux amd64, PostgreSQL 18.6, and required shard 3/3. Four required cases passed; all nine installation observations match candidate files to installed files and carry the same payload digest. The release record has five passing required suites, all 21 required cases observed passing, and no missing or incomplete suites. Topology logs retain both server identities, data directories, publication/subscription/slot names, the interruption boundary, and exact subscriber convergence. | `evidence/pr1117-release-evidence.json` (SHA-256 `e83b0b3d1d39cefde9df3668915c18d0a4b35c0aad5d8763e5f0f09e1d0bfdd6`); `evidence/pr1117-publication-recovery-installation.json` (SHA-256 `1c0bcaf6ef759e546e091619a93a6581e208b502b206393e85009b1b8a9a1d06`); retained suite log SHA-256 `c2c87843a5536f87d00f28f5275771226fd4156ee95463d0188aaabfb57670d6`; artifact ID 10909823446 | proven |

## Verification evidence

- CI run 36252436204 completed successfully on PR head `710e657f17548fb7446e1b2384b595c3a530e162`; the run's merge commit is `2e84d8b58f2a48b66d72479b71010f66efd66451`, with parents base `edf700efdbb80983fe12c0e10f84fa271b818faf` and PR head. [Run](https://github.com/trickle-labs/pg-trickle/actions/runs/36252436204); [release runtime controls](https://github.com/trickle-labs/pg-trickle/actions/runs/36252436204/job/108433655105).
- `python3 scripts/run_release_proof.py --package-dir target/release/pg_trickle-pg18 --candidate-commit "$GITHUB_SHA"` passed. Artifact `release-proof-controls` (ID 10909823446) reports all required release suites and cases passing. An independent local assertion over the downloaded artifact checked the 21/21 required case statuses, 5/5 suite statuses, shard 3/3 metadata, all nine exact installed package manifests, and empty missing/incomplete-suite lists.
- The publication suite command was `bash scripts/run_light_e2e_tests.sh --no-capture --test e2e_publication_crash_recovery_tests`: 9 passed, 0 skipped; all four contract-required cases passed.
- Light E2E shards 1/3, 2/3, and 3/3 passed: 485, 441, and 518 tests ran; 18 were skipped by the existing filters. Benchmark regression, sensitive E2E gate, integration tests, package light-E2E extension, release evidence contract, unit tests, Windows compile, shipping-image upgrade, upgrade completeness, lint, Semgrep, and secret scan passed. Schedule/manual-only jobs were skipped as configured.
- Local `just fmt`, `just lint`, `python3.12 scripts/v0_108_0_release_gate.py`, JSON parsing, `bash -n`, and `git diff --check` passed. Clippy used `-D warnings` with no warnings.
- Remote PR head is unchanged at `710e657f17548fb7446e1b2384b595c3a530e162`; the base is `edf700efdbb80983fe12c0e10f84fa271b818faf`. The repair delta is limited to four qualification/test-wiring files.

## Unresolved gaps

None for contract v1. The release evidence qualifies the current PR candidate package; it does not claim publication of a new release.

## Repairs needed

None.

## Next steps

1. `/merge-readiness https://github.com/trickle-labs/pg-trickle/pull/1117; review .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/review.md; proof .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/proof.md`.
