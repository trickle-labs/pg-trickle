# CI repair report — PR #1117

## Target and contract

- PR: https://github.com/trickle-labs/pg-trickle/pull/1117
- Intended operation: restore PR validation for the publisher-to-subscriber recovery qualification.
- Acceptance contract: `work/issue-1096-real-publisher-to-subscriber-recovery.md` v1, SHA-256 `b7788d82e9d17a04574f298295bc8c0582adb61195505a76b370a3f9779da622`.
- Comparison base: `edf700efdbb80983fe12c0e10f84fa271b818faf`.
- Old PR candidate: `f2eede5ea148c87b4d9612118ee5e85c04167df1`.
- Repaired product candidate: `710e657f17548fb7446e1b2384b595c3a530e162`.
- Classification: `PRODUCT` — qualification metadata and test-target selection did not match their runners.

## Earliest causal failures

Run [36248652150](https://github.com/trickle-labs/pg-trickle/actions/runs/36248652150) checked out merge SHA `798799608433268c4fdd0603003931f707d49d98`, containing PR head `f2eede5ea148c87b4d9612118ee5e85c04167df1`.

1. **Release evidence runtime controls**, job [108423957960](https://github.com/trickle-labs/pg-trickle/actions/runs/36248652150/job/108423957960), step `Prove package identity and required case controls`:
   - Command: `python3 scripts/run_release_proof.py --package-dir target/release/pg_trickle-pg18 --candidate-commit "$GITHUB_SHA"`.
   - The retained `release-proof-controls` artifact (ID `10908438664`) contains a `sensitivity-baseline` result at shard `1/2`, while the qualification contract requires shard `1/3`. Replaying `release_evidence.py` with those retained suite results exits 2 with: `release_evidence.py: error: qualification shard 'sensitivity-baseline' has the wrong position`.
   - The proof runner also omitted the newly required `publication-recovery` suite, so it could not provide evidence for the third shard and its four required cases.

2. **Light E2E tests (2/3)**, job [108423957941](https://github.com/trickle-labs/pg-trickle/actions/runs/36248652150/job/108423957941), step `Run light-E2E test suite`:
   - Command: `bash ./scripts/run_light_e2e_tests.sh --shard-index 2 --shard-count 3`.
   - 251 of 254 executed tests passed. Three publication recovery tests attempted to start `pg_trickle_e2e:latest`, which this stock-PostgreSQL light runner does not build. Docker reported: `pull access denied for pg_trickle_e2e, repository does not exist or may require 'docker login'`.
   - This is independent of the shard metadata failure. The publication suite is already configured to run against the package-built candidate image in the release proof runner.

The failure evidence is summarized here; the GitHub run and artifact links are the retained remote references. The `release-proof-controls` artifact has 30-day retention.

## Repair

- Added `publication-recovery` to the package runtime proof suite list, so its four required cases run against the candidate image and satisfy the third shard.
- Removed `e2e_publication_crash_recovery_tests` from automatic stock-image light sharding. The suite remains explicitly selected by the package runtime proof runner.
- Aligned the `sensitivity-baseline` and `recovery` suite shard counts with the three-shard qualification contract.
- Strengthened the v0.108.0 release gate to reject mismatches between suite shard metadata and the qualification shard contract.

No tests, assertions, failure conditions, or required suites were removed. The repaired release proof exercises the publication suite through the candidate-image path and preserves its required case checks.

## Verification

- `just fmt` — passed.
- `just lint` — passed with `PATH` selecting Python 3.12 and Bash 5; Clippy used `-D warnings` and emitted no warnings. The default host Python 3.9/Bash 3.2 could not run repository lint scripts.
- `python3.12 scripts/v0_108_0_release_gate.py` — passed, including the shard consistency assertion.
- `python3.12 -m json.tool tests/release/v0.108.0-qualification.json` — passed.
- `bash -n scripts/run_light_e2e_tests.sh scripts/run_release_proof.py scripts/v0_108_0_release_gate.py` — passed.
- `git diff --check` on the four changed files — passed.
- Original run had the two failures above; its benchmark regression check was still in progress at last inspection.
- Remote verification for repaired candidate `710e657f17548fb7446e1b2384b595c3a530e162` is pending push and a fresh PR workflow run.

## Requirements and handoff

- Affected evidence: Q1096-R02 through Q1096-R09. Q1096-R01 and the contract promises were not changed; no acceptance gaps were identified.
- Existing review and proof are bound to snapshot `snapshot:sha256:6d5632462facd43596182422371ede5cac6751577f1ee8287c82190eaa63a140`; they are stale for the repaired candidate. Fresh `/review-implementation` against the comparison base and `/prove` against every Q1096 requirement are required before acceptance.
- The base branch is not protected and the repository ruleset returned no required status-check rules. The fresh PR workflow still needs to pass every check it runs for the pushed repaired head.
