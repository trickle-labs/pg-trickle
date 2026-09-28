# REVIEWED: #1098 — unsafe boundaries under assertions and sanitizers

Contract: `work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers.md`, v1, SHA-256 `54cb9d4f028518242e577a4b03f372371a91e9188a517f2d03d6e6d37dc4e450`.

Parent context: #1090 v1 snapshot SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`; contribution to Q1090-R01, R03, and R05. The #1092 and #1093 binding snapshots match `candidate.json`. Approved route is independent to `main`; the slicing-plan SHA-256 is `31c24b4de0cbf6efecdde6a6da739f69733dbc00c70b4424cd9ae94fa099d43e`, and the approval-receipt SHA-256 is `e407062b7dae5e69bb2ce2dd2ae4ae5cbcbf1f1bbef24dc218ad58c89c0994b2`.

Candidate: commit `f868e873475fdb3fbefa48154972704a8c8cc56d`, recoverable from the issue branch and `candidate.json`.

Comparison: base and merge base `2dfe8dae90452120cf6760b71dd0def8c241756b`. The fixed-point change covers Q1098-R01–R09. Relative to the previously reviewed candidate `88184ca52c133897c27afaafb35d2738748c22fc`, this candidate changes only `tests/Dockerfile.e2e-asan`.

Stability: HEAD matches the candidate commit. The contract and all five binding-input hashes in `candidate.json` match.

Coverage: Q1098-R01–R09, including the assertion-enabled ASan image, invalid-memory probe, tuple-copy workload, owner and caller context recovery, worker cancellation and restart, diagnostic checks, CI routing, and release qualification separation.

## Contract fidelity

No material findings. The two added Docker build commands set `detect_leaks=0` only in their respective build layers. The image-level `ASAN_OPTIONS=...:detect_leaks=1` remains in effect for runtime qualification.

## Scope and simplicity

No material findings. The change is limited to the build-tool commands implicated by the LeakSanitizer failure and leaves runtime sanitizer settings intact.

## Engineering quality

No material findings. Non-blocking repository convention note: the commit subject `Fix ASan build leak detection scope` lacks the Conventional Commit type required by `CONTRIBUTING.md`. This does not affect the source change.

## Checks and limitations

Verified the candidate and contract identities, all five binding-input hashes, and the approved route hashes. The delta from the prior reviewed candidate is limited to the two build-stage environment exports. `git diff --check` for that delta and `git show --check` pass. A base-to-candidate whitespace check reports trailing spaces in the Markdown contract's hard line breaks; no whitespace issue was found in the product patch.

This review does not establish a successful qualification run for the current candidate. No tests were run in this review stage; Q1098-R09 execution evidence remains for proof to verify.

## Handoff

No material finding IDs. Review only; acceptance proof and merge readiness are separate.

Report storage: `.p2p/work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers/review.md`, save and reread pending.

## Next steps

1. Run `/prove work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers.md; candidate f868e873475fdb3fbefa48154972704a8c8cc56d` and verify the candidate-bound qualification artifact, especially Q1098-R09.
2. After matching full review and proof reports exist, no further action is required unless publication is wanted.
