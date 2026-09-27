# PR #1121 CI repair

**Target:** https://github.com/trickle-labs/pg-trickle/pull/1121 (`issue/1119`)
**Comparison base:** `2dfe8dae90452120cf6760b71dd0def8c241756b`
**Previous candidate:** `5bd0b4d107ad8d422040879a918f7518b29e5a20`
**Repair candidate:** `d2379beec8d6e3071590156474680d28f3877ca1`
**Contract:** `work/issue-1119-immediate-downstream-consistency.md` v1, SHA-256 `c7ecbeb273d2fd8ed8ef6ef3cc31fd0f2128550049a1daea93896bf7626592b8`

## Outcome

The Sensitive E2E product failure is fixed. The pinned Gitleaks finding was a public snapshot digest in an old P2P review record, not a credential. `.gitleaksignore` excludes only that exact finding fingerprint; the rest of the scanner remains active. The candidate's required GitHub checks are pending push.

## Failures and causes

### Sensitive E2E — product defect

Run [36323915476](https://github.com/trickle-labs/pg-trickle/actions/runs/36323915476), job `108632932354`, ran:

```text
cargo nextest run --profile ci --test 'e2e_*' -E 'not binary(/^e2e_refresh_atomicity_tests$/)'
```

`test_pg_mdm_compiler_v9_bootstrap_and_mutations_match_full_without_fallback` failed three times. After an update that changed a source timestamp but left the projected name value unchanged, `blocks/prefix_name` contained one extra `nul` row. The independent full-query oracle expected five rows and found six in the stream table.

The full-refresh UNION ALL builder hard-coded `SCAN_KEY` for every branch. The differential DVM uses the branch's actual identity domain; this branch used `JOIN_KEY`. Their nested row IDs differed, so downstream capture could not match the old stored row to its DELETE delta. Full refresh now shares the same branch identity builder as the regular full-refresh path, preserving the domain selected for joins, keyless scans, windows, and aggregates.

### Gitleaks — exact false positive

Run [36323915533](https://github.com/trickle-labs/pg-trickle/actions/runs/36323915533), job `108632898693`, ran:

```text
gitleaks detect --source . --log-opts "2dfe8dae90452120cf6760b71dd0def8c241756b..5bd0b4d107ad8d422040879a918f7518b29e5a20" --config .gitleaks.toml --redact --exit-code 1
```

The single redacted finding was `generic-api-key` at line 42 of `.p2p/work/issue-1119-immediate-downstream-consistency/review.md`. That line records an older `snapshot:sha256:` candidate identifier. Its exact Gitleaks fingerprint is listed once in `.gitleaksignore`; no path, rule, or other findings are allowlisted.

## Verification

- `just fmt` — passed.
- `just lint` — passed with zero warnings.
- `just build-e2e-image` — passed; image rebuilt from this candidate.
- `PGT_DISABLE_NEXTEST=1 scripts/run_e2e_tests.sh --test e2e_pg_mdm_compiler_v9_tests test_pg_mdm_compiler_v9_bootstrap_and_mutations_match_full_without_fallback -- --nocapture` — passed (1 test).
- Pinned Gitleaks v8.27.2 preflight on the previous PR diff, with the exact finding fingerprint ignored — passed (`no leaks found`). GitHub verification for the repair candidate remains pending until push.

The repaired code changes only `src/dvm/mod.rs` and `.gitleaksignore`. CI jobs and failure conditions are unchanged. Contract v1 and requirements R1–R8 are unchanged; the MDM oracle is additional regression evidence for differential row identity and downstream consistency.

The previous proof and review reports bind `snapshot:sha256:08f36577aca0522020d99d2bad996917b98746f08d0875bb06fb3596176405c9`, so they are stale for this repair candidate.

## Next steps

1. Push the candidate and verify the Sensitive E2E and Gitleaks checks on the exact PR head.
2. Refresh proof for `work/issue-1119-immediate-downstream-consistency.md` against the repaired candidate and run `/review-implementation <candidate> against 2dfe8dae90452120cf6760b71dd0def8c241756b`.
