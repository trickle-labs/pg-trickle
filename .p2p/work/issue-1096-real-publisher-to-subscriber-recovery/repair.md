# REPAIRED: issue #1096, Q1096-R09

Contract: `work/issue-1096-real-publisher-to-subscriber-recovery.md` v1  
Contract SHA-256: `b7788d82e9d17a04574f298295bc8c0582adb61195505a76b370a3f9779da622`

Candidate before: `snapshot:sha256:7b3ee81b8d44f2955f82a6422d7e46e1762ea669d88f79593e3f49fa86f03316`  
Candidate after: `snapshot:sha256:6d5632462facd43596182422371ede5cac6751577f1ee8287c82190eaa63a140`

Addressed requirements: Q1096-R09.

Changed files:

- `scripts/run_light_e2e_tests.sh` passes `--no-capture` without the redundant `--test-threads 1` argument. Nextest documents `--no-capture` as serial execution, and the final run showed each test starting after the previous test passed.
- `scripts/v0_108_0_release_gate.py` requires the machine-format branch to retain `--no-capture` for this qualification.

Changed evidence:

- Machine-format run `light-e2e-15159-1790426434`, Nextest run `431382b2-78b2-4e21-b565-d9d3189f86e8`, PostgreSQL 18.6. All 9 tests passed, including all four publication recovery cases.
- The run produced 9 fresh installation observations. Each listed 150 candidate files, and every installed payload digest matched `df3bd1d53fb3d5cdc05e4bfa3181695eafd0f6ee062f305e0e9059b1b3644430`. Eight observations came from topology servers; one came from the shared E2E harness.
- `final-machine-format-e2e.log` SHA-256: `75daf4cc01470c9dad4abedce54ef3e52b8193bb4afb8f9e06ec6dcb454f8476`.
- `final-machine-format-attestations.json` SHA-256: `918a4ba80e34acab43f47bb6d19c8301ddc6bcc2da136a2b8aa787a006fdac4b`.
- Snapshot reconstruction matched all 1,552 candidate paths, bytes, and modes against the working tree. Compared with the 1,541-file base, it contains 11 additions, 10 modifications, and no deletions. `final-candidate-reconstruction.log` SHA-256: `80a8982785f738993f0cdafe4d6196dbbb764c44f6938408b7bcf51693005edd`.

Focused checks: `just fmt`, `just lint`, `python3 scripts/v0_108_0_release_gate.py` (10 unit checks plus qualification gates), `python3 -m json.tool tests/release/v0.108.0-qualification.json`, and `bash -n scripts/run_light_e2e_tests.sh` passed. The final packaged E2E command used `PGT_RELEASE_MACHINE_FORMAT=1` and did not disable Nextest.

Remaining gaps: The run is a local Linux ARM64 package check for this uncommitted snapshot. The attestation's formal `candidate_commit`, `artifact_id`, `artifact_digest`, and `platform` fields are null. The saved candidate key and package payload digest bind the observed files to this candidate, but the run does not claim qualification of a published release artifact. Fresh review and proof must assess this limit against the exact contract.

Fresh `/prove` required before acceptance.

## Next steps

1. Run `/review-implementation .git/p2p-issue-1096-readonly/candidate.json against edf700efdbb80983fe12c0e10f84fa271b818faf`.
2. Run `/prove work/issue-1096-real-publisher-to-subscriber-recovery.md; candidate .git/p2p-issue-1096-readonly/candidate.json`.
