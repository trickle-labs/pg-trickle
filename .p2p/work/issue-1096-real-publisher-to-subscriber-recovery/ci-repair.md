# CI repair report — PR #1117

## Target and contract

- PR: https://github.com/trickle-labs/pg-trickle/pull/1117
- Outcome: `FIXED`
- Classification: `PRODUCT` — qualification metadata and test-target selection did not match their runners.
- Acceptance contract: `work/issue-1096-real-publisher-to-subscriber-recovery.md` v1, SHA-256 `b7788d82e9d17a04574f298295bc8c0582adb61195505a76b370a3f9779da622`.
- Comparison base: `edf700efdbb80983fe12c0e10f84fa271b818faf`.
- Original failing PR head: `f2eede5ea148c87b4d9612118ee5e85c04167df1`.
- Repaired PR head: `710e657f17548fb7446e1b2384b595c3a530e162` (`fix(ci): qualify publication recovery on candidate image`).

## Earliest causal failures

The original workflow run [36248652150](https://github.com/trickle-labs/pg-trickle/actions/runs/36248652150) checked PR head `f2eede5e` merged with base `edf700ef`.

1. **Release evidence runtime controls**, job [108423957960](https://github.com/trickle-labs/pg-trickle/actions/runs/36248652150/job/108423957960): `python3 scripts/run_release_proof.py --package-dir target/release/pg_trickle-pg18 --candidate-commit "$GITHUB_SHA"` rejected `sensitivity-baseline` because its qualification shard said `1/2` while the contract required `1/3`. The runner also omitted the required `publication-recovery` suite and its four cases.
2. **Light E2E tests (2/3)**, job [108423957941](https://github.com/trickle-labs/pg-trickle/actions/runs/36248652150/job/108423957941): `bash ./scripts/run_light_e2e_tests.sh --shard-index 2 --shard-count 3` had 3 publication-recovery tests try to start the absent `pg_trickle_e2e:latest` image. The stock light runner does not build that custom image; 251 other tests passed. This was independent of the shard mismatch.

## Repair

- Added the publication recovery suite to release proof so it runs against the packaged candidate image.
- Removed that suite from the stock-image light E2E sharding path.
- Aligned recovery and sensitivity qualification shard metadata to `2/3` and `1/3`.
- Added a release-gate assertion that all required shard metadata matches the qualification contract.

The required suite, four cases, test assertions, and failure semantics remain intact.

## Verification

Local checks passed on the repaired commit:

- `just fmt`
- `just lint` (Python 3.12 and Bash 5 selected; Clippy denied warnings)
- `python3.12 scripts/v0_108_0_release_gate.py`
- Qualification JSON parsing, `bash -n` for affected runner scripts, and `git diff --check`.

Fresh GitHub workflows completed successfully for PR head `710e657f17548fb7446e1b2384b595c3a530e162`: [CI run 36252436204](https://github.com/trickle-labs/pg-trickle/actions/runs/36252436204), Lint, Semgrep, and secret scan. The CI package proof ran against synthetic merge commit `2e84d8b58f2a48b66d72479b71010f66efd66451`; GitHub API confirmed its parents are exactly base `edf700ef` and PR head `710e657f`.

- Release runtime controls passed. Artifact `release-proof-controls` ID `10909823446` has all five required suites and 21 required cases passing, no missing or incomplete suites, publication suite shard `3/3`, and four required publication cases passing. Nine installation observations match candidate and installed package file manifests.
- Light E2E shards 1/3, 2/3, and 3/3 passed (485, 441, and 518 tests ran; 18 skipped by existing filters).
- Benchmark regression, sensitive E2E gate, integration tests, unit tests, Windows compile, package setup, shipping-image upgrade, upgrade completeness, and release contract checks passed. Scheduled/manual-only jobs were skipped as configured.
- The PR remains at head `710e657f`; base remains `edf700ef`. The repository ruleset has no required status-check rule; all applicable PR checks passed.

Retained proof evidence is under `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/`. The release evidence JSON SHA-256 is `e83b0b3d1d39cefde9df3668915c18d0a4b35c0aad5d8763e5f0f09e1d0bfdd6`; the exact publication suite log, installation record, and selected CI log excerpt are also saved there.

Fresh `/prove` and `/review-implementation` reports both bind to candidate `710e657f17548fb7446e1b2384b595c3a530e162` and base `edf700efdbb80983fe12c0e10f84fa271b818faf`; proof is `PROVEN` for all nine requirements and review is `REVIEWED` with no material findings. No acceptance gap remains for contract v1.

## Report storage

This report is saved locally at `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/ci-repair.md` and is not part of the pushed product commit. A prior attempt to make a separate report-only commit was rejected by automatic approval review as an unrequested documentation-only commit. I did not retry it; the code repair commit and PR verification are unaffected.

## Next steps

1. `/merge-readiness https://github.com/trickle-labs/pg-trickle/pull/1117; review .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/review.md; proof .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/proof.md`
