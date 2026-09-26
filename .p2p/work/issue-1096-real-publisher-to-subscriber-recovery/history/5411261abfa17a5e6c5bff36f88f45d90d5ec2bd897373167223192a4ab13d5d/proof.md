# PROVEN: issue #1096 — real publisher-to-subscriber recovery

**Result:** PROVEN for the fixed local candidate snapshot and supplied package. This does not qualify a published release artifact.

## Fixed identities

- Contract: revision v1, SHA-256 b7788d82e9d17a04574f298295bc8c0582adb61195505a76b370a3f9779da622
- Candidate: snapshot:sha256:6d5632462facd43596182422371ede5cac6751577f1ee8287c82190eaa63a140
- Candidate manifest SHA-256: e300048623e660f02317b1a679f2a442095141daa5298514dce7361070bb4616
- Base: edf700efdbb80983fe12c0e10f84fa271b818faf; archive SHA-256 ee8650687d9fdd6cf520a559140024ea3960b18f6a18377910d102611286d52e
- Reconstructed all 1,552 candidate paths, bytes, and modes. Base has 1,541 files; candidate adds 11, modifies 10, deletes 0.
- Candidate image: pg_trickle_release_candidate:local, sha256:77aca7d14e28bda56cbc3dec5b94600876b65997d61891a1b6f230257adc2fb4, linux/arm64.

## Requirement verdicts

| ID | Result and observed evidence |
|---|---|
| Q1096-R01 | PROVEN. Legacy wording and comments accurately describe publisher connection-recovery smoke coverage; release qualification describes the new topology suite. |
| Q1096-R02 | PROVEN. Distinct publisher/subscriber instances, real publication and subscription, ready table sync, and exact initial rows (1,a,10), (2,b,20). |
| Q1096-R03 | PROVEN. Insert/update/delete converge to exact rows (1,a,11), (3,a,30); a subsequent batch adds (4,a,40). Values and multiplicity are compared. Full catalog schema metadata is not separately compared. |
| Q1096-R04 | PROVEN. Subscriber outage leaves slot backlog at LSN 0/1D29F50. The same container, cluster identity, and data directory restart with a new postmaster; rows converge exactly. |
| Q1096-R05 | PROVEN. Publisher crash/restart preserves its data identity, publication, and slot; pre-restart and post-restart batches reach the subscriber exactly. |
| Q1096-R06 | PROVEN. Apply worker PID 1903 is blocked by PID 84 while IDs 1,2,4 are delivered and 50,6 remain pending. Subscriber interruption/restart converges at checkpoint LSN 0/1D41910 and the slot catches up. |
| Q1096-R07 | PROVEN. Missing-row and altered-value controls fail exact comparison. Disabled progress leaves rows unchanged and backlog outstanding; re-enabling delivers row 7. The disabled-progress detector uses a 2-second wait, not the full 120-second limit. |
| Q1096-R08 | PROVEN. Missing stream registration and unreachable publisher setup fail independently; failed subscription leaves no orphan row and no publisher-only success fallback. |
| Q1096-R09 | PROVEN for local topology qualification. Each extension-bearing server has an install attestation, identity/version, and logs; publication/subscription/slot names, restart identities, backlog/checkpoint LSNs, interruption, and exact comparison are recorded. All installed files match the candidate package. Published artifact qualification is unproven because formal release identity fields are null. |

## Verification evidence

The final machine-format runner used Nextest (not disabled), with Python 3.14.7 selected. It completed serial START/PASS order 1/9 through 9/9: 9 passed, 0 skipped, in 119.392 seconds. The first attempt stopped before tests because bash resolved Python 3.9.6; its incomplete attestation is excluded.

- Run log: evidence/native-proof-run-final.log, SHA-256 311e7078686940d8407ebc4684a193004af3a6e4659396288a86c4eecdc69a15
- Install attestations: evidence/native-proof-attestation-final.json, SHA-256 1629cd2914ccc0ebcc18039b98361559346f3769f00f3dfabdc6d4dcbaf06a08
- Nine attestation rows, each with 150 candidate and installed files. Every path, byte count, and digest matches. Package payload digest: df3bd1d53fb3d5cdc05e4bfa3181695eafd0f6ee062f305e0e9059b1b3644430. All servers ran PostgreSQL 18.6.

## Qualification limit

This was an ARM64 run against an uncommitted local snapshot. Attestation fields candidate_commit, artifact_id, artifact_digest, and platform are null. The evidence proves installation and execution of the supplied package; it does not bind a published release artifact, commit, or platform.
