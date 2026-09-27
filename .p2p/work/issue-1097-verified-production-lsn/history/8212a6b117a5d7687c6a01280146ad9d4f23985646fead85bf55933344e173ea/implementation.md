# PARTIAL: issue #1097 — Verify and integrate one production LSN implementation

Contract: `work/issue-1097-verified-production-lsn.md`, revision v1, SHA-256 `dae07ec8c073477626add9b70a114891b5bbe7431e9790537c075bd731cc4259`.

Parent context: #1090 v1, exact contract SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`. Contribution maps to Q1090-R03, Q1090-R05, and Q1090-R06. Saved #1091 and #1093 prerequisite snapshots remain the bound inputs.

Scope: Q1097-R01–Q1097-R11.

Candidate before: prior snapshot `snapshot:sha256:3e8a030c39c44af4c090e4677ddcacd5a4659d422ff17a20e86edbb902df1018` (stale for the current worktree).

Candidate after: not captured. The parent #1090 has no approved `slicing.md` route or approval receipt. The earlier base `6dc7e07c6bf53df2bf2327c4146cb140ca433f64` is no longer the current `main` tip, now `2dfe8dae90452120cf6760b71dd0def8c241756b`. The route and comparison base need resolution before a final candidate can be captured.

Changes: checked LSN parser, formatter, and numeric comparisons are integrated into the version/frontier, scheduler, CDC, recovery, refresh, and WAL paths. Verus and CI enforcement bind the implementation, package library, and runtime cases. Package E2E coverage now exercises integrated callers, malformed-frontier scheduler preservation and recovery, CDC writer holdback, and WAL transition boundaries.

## Requirement handoff

| ID | Implementation / observed evidence | Remaining gap |
|---|---|---|
| Q1097-R01 | Executable checked parser; pinned Verus proves value and bounds; unit suite passed 2,590 tests. | None observed. |
| Q1097-R02 | Verified formatter/parser round trip and unit/property boundaries; unit suite passed. | None observed. |
| Q1097-R03 | Numeric LSN helpers and PostgreSQL `pg_lsn` oracle in packaged E2E; full LSN target passed 7/7. | None observed. |
| Q1097-R04 | Persisted unpadded LSN compatibility exercised by the packaged integrated-caller case. | None observed. |
| Q1097-R05 | Caller inventory and shared production module integrated across named callers. | Fresh full review pending. |
| Q1097-R06 | Packaged scheduler case preserved malformed progress, rows, CDC buffer, and refresh timestamp, then recovered after restoring valid progress; CDC and WAL boundary cases passed. | Fresh candidate-bound proof pending. |
| Q1097-R07 | Full LSN package E2E target passed 7/7; installed-library attestation was produced. | Final candidate identity and binding record pending. |
| Q1097-R08 | Pinned Verus baseline: 44 verified, 0 errors; executable semantic mutation rejected. | None observed. |
| Q1097-R09 | Verifier runner checks required obligations and CI gating; candidate-binding negative controls passed 4/4. | Recheck candidate-bound controls after final capture. |
| Q1097-R10 | Specification assumptions and independent review evidence exist for the prior candidate. | Fresh review against final source/candidate pending. |
| Q1097-R11 | Package and installed library digests matched in the previous candidate binding. | Rebuild/re-attest and bind to the final candidate after route resolution. |

## Checks and limitations

- `just fmt` and `just lint`: passed.
- `just test-unit`: sandboxed run had three local-socket permission failures; escalated rerun passed 2,590/2,590.
- `cargo test --features pg18 --test e2e_lsn_contract_tests --no-run`: passed.
- `python3 scripts/test_lsn_candidate_binding.py`: 4/4 passed.
- Pinned `python3 scripts/check_lsn_verification.py`: baseline passed (44 verified, 0 errors); semantic mutation rejected.
- Packaged full LSN E2E target: 7/7 passed, including all four required LSN runtime cases. Focused packaged light E2E was previously 4/4; general light E2E was not complete.
- Earlier integration run: 161/161 passed; retained evidence is `.p2p/work/issue-1097-verified-production-lsn/evidence/just-test-integration-final.log`.
- Candidate/package attestation and review/proof reports currently in `.p2p/work/issue-1097-verified-production-lsn/` refer to an earlier snapshot and are not evidence for the final worktree.

## Decisions and next step

The final comparison base is blocked on the missing approved parent delivery route. Resolve that route through `/slice-contract #1090` or provide the existing approved plan. Then capture a fresh candidate from the approved target, rebuild/re-attest the package, and run separate full review and proof contexts against that candidate.

Report storage: `.p2p/work/issue-1097-verified-production-lsn/implementation.md`.

Implementation report only; independent acceptance requires matching current-candidate review and proof.
