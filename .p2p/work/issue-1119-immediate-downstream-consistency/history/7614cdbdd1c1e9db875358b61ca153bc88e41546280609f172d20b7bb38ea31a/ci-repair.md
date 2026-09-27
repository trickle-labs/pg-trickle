# PR #1121 CI repair

**Target:** `issue/1119`  
**Current candidate:** `e8bca56d339ec69a6ee1199af20d3853b6bb84e4`  
**Base:** `2dfe8dae90452120cf6760b71dd0def8c241756b`

## Repairs

- Updated release evidence and upgrade checks from v0.108.0 to v0.108.1, including the new qualification contract and release gate.
- Excluded pg_trickle-owned DML guard triggers from application-trigger classification, preserving the vectorized refresh path.
- Preserved stable row identity for keyless stream-table scans while avoiding an extra payload hash for base-table scans.
- Kept the scheduler-only E2E test out of the lightweight compile target.
- Added narrow Semgrep annotations to catalog-quoted dynamic trigger SQL.
- Removed the credential-like local Citus URL from the justfile example.
- Added narrowly scoped Gitleaks exceptions for the historical justfile sample and two retained E2E logs.

## Local checks

- `just fmt` — passed.
- `just lint` — passed with zero warnings.
- `just test-unit` — 2,588 passed, 0 failed.
- `python3 scripts/v0_108_1_release_gate.py` — passed.
- `scripts/check_upgrade_completeness.sh 0.108.0 0.108.1` — passed.
- Focused keyless diff-scan tests — 28 passed.
- `cargo check --test e2e_ivm_tests --features pg18,light-e2e` — passed.
- Local E2E execution was unavailable because Docker was not running.

## CI status on current candidate

Run `36317102206` is still running. Unit, integration, upgrade, Windows compile, lint, Semgrep, fuzz, dependency, security audit, docs, unsafe inventory, v0.108.1 release contract, v0.87 pgbench overhead, release runtime controls, and all three light E2E shards pass. The sensitive E2E gate and benchmark regression check remain in progress.

Secret scanning run `36317102288` failed with one finding. Its safe metadata output did not provide a rule ID or path. No matched value was printed. The local sample URL and two known E2E log locations have narrow commit-and-path exceptions; the likely remaining duplicate is the generated candidate manifest. Its 1,556 embedded files were hash-checked and all match ordinary tracked files in the introducing commit. Auto-review rejected adding the manifest exception because it would persistently bypass scanning for that path. The change was reverted, and explicit user approval is pending.

## Acceptance evidence

The existing proof and review reports bind candidate snapshot `08f36577…`; they are stale for this CI-repair candidate. After the final commit, rerun:

```text
/prove work/issue-1119-immediate-downstream-consistency.md; candidate <final SHA>
/review-implementation <final SHA> against 2dfe8dae90452120cf6760b71dd0def8c241756b
```
