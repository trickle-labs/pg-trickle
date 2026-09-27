# PR #1121 CI repair — NOT FIXED

**Target:** https://github.com/trickle-labs/pg-trickle/pull/1121 (`issue/1119`)
**Base:** `2dfe8dae90452120cf6760b71dd0def8c241756b`
**Current candidate:** `5bd0b4d107ad8d422040879a918f7518b29e5a20`
**Previous candidate:** `a56485e3b8d982fd9d88cc0d74c68acff0b31ef7`
**Contract:** `work/issue-1119-immediate-downstream-consistency.md` v1, SHA-256 `c7ecbeb273d2fd8ed8ef6ef3cc31fd0f2128550049a1daea93896bf7626592b8`

## Outcome

NOT FIXED. The latest candidate passes all three Light E2E shards and the benchmark regression check, but the Sensitive E2E gate and Gitleaks fail. No further source edit was made because the remaining MDM defect has not been isolated to a correct repair.

## Failures

### Sensitive E2E: product behavior failure

Run [36323915476](https://github.com/trickle-labs/pg-trickle/actions/runs/36323915476), job `108632932354`, ran:

```text
cargo nextest run --profile ci --test 'e2e_*' -E 'not binary(/^e2e_refresh_atomicity_tests$/)'
```

`test_pg_mdm_compiler_v9_bootstrap_and_mutations_match_full_without_fallback` failed three times at `tests/e2e/oracle.rs:734`. After updating CRM row 2's email and birth date, `blocks/prefix_name` had 6 rows while the independent full-query oracle had 5: one extra `field_name="name"`, `channel_id="prefix_name"`, `block_key="nul"` row for source record `781c51ec-06ad-b983-7319-b7f877d9458a`. The fixture's upstream `normalized/name` includes `row_changed_at`; `blocks/prefix_name` projects that field away, so the D+I must cancel after projection. The current candidate does not satisfy that invariant. The exact cause in the differential refresh/cascade path remains unresolved.

The same oracle failure was present on earlier branch candidates. The initial scan identity change also regressed two Light E2E cases; the current narrower scan behavior restored all three Light shards but did not resolve this separate deterministic failure. `test_auto_refresh_within_schedule` failed one timed attempt and passed its next attempt; it is not the remaining blocker.

### Gitleaks: unresolved finding

Run [36323915533](https://github.com/trickle-labs/pg-trickle/actions/runs/36323915533), job `108632898693`, ran:

```text
gitleaks detect --source . --log-opts "2dfe8dae90452120cf6760b71dd0def8c241756b..5bd0b4d107ad8d422040879a918f7518b29e5a20" --config .gitleaks.toml --redact --exit-code 1
```

It exited 1 with `leaks found: 1`. The redacted job log does not expose the rule or path, and no matched value was read or recorded. A prior proposed path allowlist for the generated candidate manifest was rejected by automatic approval review because it would create a persistent scanning bypass; that exception was reverted and is absent from the candidate.

## Verification on the exact candidate

- `just fmt` — passed.
- `just lint` — passed with zero warnings.
- `./scripts/run_unit_tests.sh pg18 test_diff_scan_stream_table_keyless` — 2 passed.
- `./scripts/run_unit_tests.sh pg18 test_is_rowwise_scan_tree` — 2 passed.
- CI — all three Light E2E shards passed; benchmark regression check passed; Sensitive E2E and Gitleaks failed. Other completed, applicable check runs passed.
- Repository ruleset `Protect main` requires pull requests and linear history; no required status-check contexts are configured. The current failures still prevent declaring this candidate repaired.

The CI repair changed `src/dvm/diff.rs`, `src/dvm/mod.rs`, and `src/dvm/operators/scan.rs` on the candidate. The latest turn made no source changes. The issue contract and requirement IDs R1–R8 are unchanged; the MDM oracle is additional regression evidence. Existing proof and review bind `snapshot:sha256:08f36577aca0522020d99d2bad996917b98746f08d0875bb06fb3596176405c9`, so both are stale for this commit.

## Next steps

1. Isolate and repair the deterministic `blocks/prefix_name` mismatch while retaining the exact full-query oracle and Light E2E coverage.
2. Identify the Gitleaks path and rule through safe metadata, then remove a real credential or correct the generated input without bypassing the scanner.
3. Push the repaired candidate, verify both failing checks on that exact SHA, then rerun `/prove work/issue-1119-immediate-downstream-consistency.md; candidate <repaired SHA>` and `/review-implementation <repaired SHA> against 2dfe8dae90452120cf6760b71dd0def8c241756b`.
