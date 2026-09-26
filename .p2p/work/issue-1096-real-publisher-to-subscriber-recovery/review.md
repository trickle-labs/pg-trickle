# REVIEWED: GitHub issue #1096

**Outcome:** REVIEWED

**Contract:** `work/issue-1096-real-publisher-to-subscriber-recovery.md`, revision v1, SHA-256 `b7788d82e9d17a04574f298295bc8c0582adb61195505a76b370a3f9779da622`.

**Candidate:** `snapshot:sha256:6d5632462facd43596182422371ede5cac6751577f1ee8287c82190eaa63a140`  
**Candidate manifest:** `.git/p2p-issue-1096-readonly/candidate.json`, SHA-256 `e300048623e660f02317b1a679f2a442095141daa5298514dce7361070bb4616`.

**Comparison base:** `edf700efdbb80983fe12c0e10f84fa271b818faf`; archive SHA-256 `ee8650687d9fdd6cf520a559140024ea3960b18f6a18377910d102611286d52e`.

**Comparison:** Reconstructed the 1,552-entry candidate in disposable scratch and compared it with all 1,541 entries in the base archive. The candidate has 10 modified files, 11 additions, and no deletions. Its paths, bytes, and modes match the saved manifest and workspace. Protected input hashes match the requested values.

## Contract fidelity

- **Q1096-R01 — Covered.** The legacy case is named and described as publisher connection-recovery smoke. Its optional publication-registration assertion is qualified, and the changelog, roadmap, migration comment, and v0.105 release gates no longer claim subscriber or postmaster recovery.
- **Q1096-R02 — Covered.** The test creates separate PostgreSQL containers on a Docker network, resolves the publication's catalog binding, creates a real subscription, waits for table readiness, and compares the subscriber's initial rows.
- **Q1096-R03 — Covered.** The subscriber is checked after insert, update, and delete, then after a subsequent batch, with exact values and multiplicity.
- **Q1096-R04 — Covered.** The subscriber is stopped while a refreshed batch accumulates. The test observes slot backlog, restarts the same container, asserts retained cluster and data-directory identity, and checks exact convergence.
- **Q1096-R05 — Covered.** The publisher is killed and restarted in its retained container. The test checks publisher identity plus publication and slot retention, then verifies exact catch-up and a post-restart batch.
- **Q1096-R06 — Covered.** A blocked logical apply worker is observed while later changes remain pending. The subscriber is interrupted, restarted in the same container, and checked against the final exact checkpoint.
- **Q1096-R07 — Covered.** The exact-row comparator rejects missing and altered rows. Disabling the subscription leaves rows unchanged and slot work outstanding; re-enabling it converges to the expected rows.
- **Q1096-R08 — Covered.** Missing stream-table registration and an unreachable publisher each fail setup; failed subscription creation leaves no orphan subscription or publisher-only pass path.
- **Q1096-R09 — Covered for this local candidate-package run.** The v0.108.0 gate requires all four topology cases, the candidate image, the publication-topology shard, and `--no-capture` in the machine-format runner.

## Prior finding F1

The earlier concurrency finding is resolved. The candidate passes Nextest `--no-capture` on the machine-format branch, and the v0.108 gate requires that argument. Nextest documents `--no-capture` as serial execution; the final run log also shows each test starting after the previous test passes. [Nextest reporting documentation](https://nexte.st/docs/reporting/)

## Evidence reviewed

- `final-machine-format-e2e.log` (SHA-256 `75daf4cc01470c9dad4abedce54ef3e52b8193bb4afb8f9e06ec6dcb454f8476`): machine-format Nextest run, 9 passed, 0 failed; the four topology tests passed sequentially.
- `final-machine-format-attestations.json` (SHA-256 `918a4ba80e34acab43f47bb6d19c8301ddc6bcc2da136a2b8aa787a006fdac4b`): nine server observations, including eight topology servers. All report the same 150-file candidate and installed payload digest, `df3bd1d53fb3d5cdc05e4bfa3181695eafd0f6ee062f305e0e9059b1b3644430`.
- `release-qualification-gate.log`: v0.108.0 qualification, evidence, publication, and negative-control gates passed.
- `fmt-lint.log`: formatting and lint passed; Clippy reported zero warnings.
- `final-candidate-reconstruction.log` (SHA-256 `80a8982785f738993f0cdafe4d6196dbbb764c44f6938408b7bcf51693005edd`): records the candidate reconstruction and comparison.

## Findings

None.

## Limitations

The run qualifies a local, uncommitted candidate package on PostgreSQL 18.6 Linux ARM64. Its attestation has null candidate commit, artifact ID, artifact digest, and platform fields, so it does not establish qualification of a published release artifact. This is an implementation review, not an acceptance proof or merge-readiness decision.
