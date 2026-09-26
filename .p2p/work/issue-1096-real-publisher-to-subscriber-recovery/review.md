# REVIEWED: issue #1096 — PR #1117 repair

Contract: `work/issue-1096-real-publisher-to-subscriber-recovery.md` v1, SHA-256 `b7788d82e9d17a04574f298295bc8c0582adb61195505a76b370a3f9779da622`  
Parent context: #1090 v1, snapshot SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`; prerequisites #1091/#1092 and release-evidence contract #1093  
Candidate: commit `710e657f17548fb7446e1b2384b595c3a530e162`; recoverable as PR head `issue/1096`  
Comparison: base `edf700efdbb80983fe12c0e10f84fa271b818faf`; complete base-to-candidate PR change set  
Stability: candidate, contract, and base remained unchanged. P2P validation ties `candidate.json` to the exact commit and base. The CI package was built from merge commit `2e84d8b58f2a48b66d72479b71010f66efd66451`, whose parents are the recorded base and candidate.
Coverage: Q1096-R01 through Q1096-R09, plus the full four-file CI repair delta. The existing implementation and test suite were covered by the previous review; I confirmed their tracked content was unchanged from `f2eede5ea148c87b4d9612118ee5e85c04167df1`, then rechecked the current base-to-head scope and the surrounding runners, contract metadata, release gate, and runtime evidence.

## Contract fidelity

No material findings. The repair moves the publication recovery test binary from automatic stock-PostgreSQL light sharding to the package runtime proof runner. The required publication suite remains required, executes against the release candidate package, and keeps its four required case IDs. The recovery and sensitivity shard metadata now matches the three-shard contract. The new release-gate assertion rejects future suite/shard metadata mismatches. The fresh artifact records publication shard 3/3, all four cases passed, nine tests executed, zero skipped; all 21 release required cases and all five suites passed.

## Scope and simplicity

No material findings. The repair changes only `scripts/run_light_e2e_tests.sh`, `scripts/run_release_proof.py`, `scripts/v0_108_0_release_gate.py`, and `tests/release/v0.108.0-qualification.json`: one test-target removal from the stock runner, one package suite addition, a six-line consistency assertion, and shard count corrections. It adds no dependency, product runtime behavior, or optional gate.

## Engineering quality

No material findings. The publication topology suite now runs in the environment it requires: the candidate package image. The normal light-E2E shards continue to pass. Release evidence preserves candidate identity, package and payload digests, suite/case statuses, shard position, installed-file manifests, and server observations. The exact-row, slot-boundary, restart, and failure-control assertions remain in the existing test code. No required test or failure condition was weakened.

## Checks and limitations

- Fresh GitHub CI run [36252436204](https://github.com/trickle-labs/pg-trickle/actions/runs/36252436204) and associated PR workflows completed successfully for head `710e657f…`. This includes release evidence runtime controls, all three light-E2E shards, benchmark regression, integration, unit, Windows compile, package setup, shipping-image upgrade, and release contract checks. Schedule/manual-only jobs were skipped as configured.
- Release artifact `release-proof-controls` (ID 10909823446) reports Linux amd64/PostgreSQL 18.6, candidate package digest `44401b1e…`, all required suites/cases passing, no incomplete suites, and exact equality of candidate/installed payload manifests across nine installation observations. The package identity is the PR workflow's synthetic merge commit with the reviewed head and comparison base as parents.
- Evidence retained under `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/`: `pr1117-release-evidence.json`, `pr1117-publication-recovery-installation.json`, `pr1117-publication-recovery.log`, and `pr1117-publication-recovery-job.log`.
- `just fmt` and `just lint` passed locally; the current PR Lint workflow passed with Clippy warnings denied. The release contract gate, JSON parse, `bash -n`, and `git diff --check` also passed.
- No review finding depends on a published release; this candidate qualifies the PR package.

## Handoff

No findings. The current candidate has matching fresh proof and review reports; acceptance evidence is complete for commit `710e657f17548fb7446e1b2384b595c3a530e162`. Review only; this is not merge approval.

Report storage: `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/review.md`, read back and verified.

## Next steps

1. `/merge-readiness https://github.com/trickle-labs/pg-trickle/pull/1117; review .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/review.md; proof .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/proof.md`
