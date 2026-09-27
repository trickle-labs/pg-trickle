# PR #1121 CI repair

**Target:** `https://github.com/trickle-labs/pg-trickle/pull/1121` (`issue/1119`)  
**Contract:** `work/issue-1119-immediate-downstream-consistency.md` v1, SHA-256 `c7ecbeb273d2fd8ed8ef6ef3cc31fd0f2128550049a1daea93896bf7626592b8`  
**Base:** `2dfe8dae90452120cf6760b71dd0def8c241756b`  
**Prior PR head:** `e8bca56d339ec69a6ee1199af20d3853b6bb84e4`  
**Repaired candidate:** `a56485e3b8d982fd9d88cc0d74c68acff0b31ef7`

## Diagnosis

Run `36317102206` exposed a product regression in Sensitive E2E: `test_pg_mdm_compiler_v9_bootstrap_and_mutations_match_full_without_fallback` failed its exact oracle after updating an upstream row's email and birth date. `blocks/prefix_name` had one extra row. The keyless stream-table scan grouped deltas by source identity and visible payload to preserve multiplicity, but emitted the source identity as the downstream row ID. An upstream change to an unused field could therefore change the ID of an unchanged downstream row.

The run also failed Secret scanning. On repaired SHA `a56485e3b8d982fd9d88cc0d74c68acff0b31ef7`, Gitleaks run `36320806531` still exits 1 with one finding. Its safe metadata does not identify a path or rule. No matched value was read or recorded. A narrowly scoped exception for the generated candidate manifest was rejected by automatic review due persistent bypass risk; it was reverted and has not been reintroduced.

## Repair

`src/dvm/operators/scan.rs` now groups stream-table keyless changes by source identity and payload, while emitting the payload identity as the downstream row ID. Base-table keyless scans continue to use the source content hash. The existing exact full-query oracle remains unchanged.

No acceptance contract text or IDs changed. The product fix reinforces downstream consistency (contract v1); the compiler-v9 graph case is additional regression evidence outside the named R1-R8 scenarios.

## Verification

- `just fmt` — passed.
- `just lint` — passed with zero warnings.
- `./scripts/run_unit_tests.sh pg18 test_diff_scan` — 28 passed.
- CI run `36320806697` is bound to candidate `a56485e3b8d982fd9d88cc0d74c68acff0b31ef7`. Unit tests, integration tests, lint, Windows compile, Semgrep, Semgrep OSS, security audit, dependency policy, fuzz replay, upgrade completeness, release evidence contract, package light E2E, shipping image upgrade, docs/catalog, and unsafe inventory have passed so far. Sensitive E2E, light E2E shards, and benchmark jobs are still running.
- Gitleaks command from `.github/workflows/secret-scan.yml`: `gitleaks detect --source . --log-opts "2dfe8dae90452120cf6760b71dd0def8c241756b..a56485e3b8d982fd9d88cc0d74c68acff0b31ef7" --config .gitleaks.toml --redact --exit-code 1` — failed with one finding.
- Read-only repository rules show required linear history and pull requests, with no required-status-check contexts configured. The PR workflow checks remain the verification signal for this repair.

## Handoff

Earlier proof and review bind `snapshot:sha256:08f36577aca0522020d99d2bad996917b98746f08d0875bb06fb3596176405c9`; they are stale for this product change. After the final candidate is verified, rerun:

```text
/prove work/issue-1119-immediate-downstream-consistency.md; candidate a56485e3b8d982fd9d88cc0d74c68acff0b31ef7
/review-implementation a56485e3b8d982fd9d88cc0d74c68acff0b31ef7 against 2dfe8dae90452120cf6760b71dd0def8c241756b
```
