# Implementation: issue #1096

Contract: `work/issue-1096-real-publisher-to-subscriber-recovery.md` v1  
Contract SHA-256: `b7788d82e9d17a04574f298295bc8c0582adb61195505a76b370a3f9779da622`  
Candidate: `snapshot:sha256:6d5632462facd43596182422371ede5cac6751577f1ee8287c82190eaa63a140`  
Comparison base: `edf700efdbb80983fe12c0e10f84fa271b818faf`

## Implementation

The publication recovery tests use separate PostgreSQL 18 publisher and subscriber containers. They check exact initial sync and insert/update/delete delivery, subscriber restart catch-up, publisher crash and retained slot recovery, and interruption while apply is blocked. Negative controls detect missing rows, altered values, disabled progress, missing stream setup, and failed subscription setup.

The release qualification lists all four topology cases. Legacy ERR-4 wording now matches the smoke test that remains in place. The machine-format Nextest path uses `--no-capture`, which runs tests serially. The v0.108 release gate now checks that this option remains enabled for the publication qualification.

## Checks

- `just fmt`: passed.
- `just lint`: passed with zero Clippy warnings. Security-definer, privilege-boundary, docs, SQL builder, fuzz inventory, and monitoring checks passed.
- `python3 scripts/v0_108_0_release_gate.py`: passed 10 unit checks and all v0.108 qualification, evidence, publication, and negative-control gates.
- `python3 -m json.tool tests/release/v0.108.0-qualification.json`: passed.
- `bash -n scripts/run_light_e2e_tests.sh`: passed.
- The final machine-format packaged PostgreSQL 18.6 run passed 9/9 tests, including all four recovery cases. Nextest started each test after the previous one passed.

The final run produced nine fresh server installation observations. Eight belong to topology servers. Each observation includes 150 files, and its installed payload digest matches the candidate package digest `df3bd1d53fb3d5cdc05e4bfa3181695eafd0f6ee062f305e0e9059b1b3644430`.

## Evidence

- Machine-format log: `evidence/final-machine-format-e2e.log`, SHA-256 `75daf4cc01470c9dad4abedce54ef3e52b8193bb4afb8f9e06ec6dcb454f8476`.
- Machine-format installation observations: `evidence/final-machine-format-attestations.json`, SHA-256 `918a4ba80e34acab43f47bb6d19c8301ddc6bcc2da136a2b8aa787a006fdac4b`.
- Candidate reconstruction: `evidence/final-candidate-reconstruction.log`, SHA-256 `80a8982785f738993f0cdafe4d6196dbbb764c44f6938408b7bcf51693005edd`.
- Earlier serial E2E run and evidence remain available as `evidence/repair-machine-format-e2e.log` and `evidence/repair-machine-format-attestations.json`.
- Full comparison-base archive: `evidence/comparison-base-edf700ef.tar.gz`, SHA-256 `ee8650687d9fdd6cf520a559140024ea3960b18f6a18377910d102611286d52e`.

This is a local Linux ARM64 package run on an uncommitted snapshot. Its attestation has null formal release commit, artifact, and platform fields. The run establishes the installed package payload and recovery behavior for this candidate; it is not a published release-artifact result. Independent review and proof remain required.
