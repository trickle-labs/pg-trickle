# CHANGES NEEDED: issue #1097 verified production LSN

Contract: [work/issue-1097-verified-production-lsn.md](/private/tmp/issue1097-candidate-b2_6bc63/work/issue-1097-verified-production-lsn.md), revision v1, SHA-256 `dae07ec8c073477626add9b70a114891b5bbe7431e9790537c075bd731cc4259`.

Parent context: #1090 v1, exact contract SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`; source snapshot SHA-256 `c65c99edb02da9d1640f3dff9189a971a2bd3414956af30ceccf273c536bab06`. The contribution maps to Q1090-R03, Q1090-R05, and Q1090-R06. The retained #1091 snapshot records 11/11 proven; #1093’s retained report records 14/14 proven. Those are prerequisite outcomes, not evidence for this candidate. The parent’s approved delivery route remains unresolved.

Candidate: `snapshot:sha256:3e8a030c39c44af4c090e4677ddcacd5a4659d422ff17a20e86edbb902df1018`; recoverable 1,564-entry manifest in [candidate.json](/private/tmp/issue1097-candidate-b2_6bc63/.p2p/work/issue-1097-verified-production-lsn/candidate.json).

Comparison: base `6dc7e07c6bf53df2bf2327c4146cb140ca433f64`, equal to checkout `HEAD`. Scope includes the complete captured product tree, including untracked files, excluding `.p2p/`.

Stability: Final read-only validation passed. The contract, five binding-input hashes, candidate key, manifest, and comparison base still match. No files were changed.

Coverage: Full contract Q1097-R01–Q1097-R11 and the complete candidate diff were inspected. The comparison is against the requested base; its status as the parent’s approved target cannot be confirmed.

## Contract fidelity

- Q1097-R01–R04: The checked parser enforces the stated byte grammar and numeric value. The formatter’s postconditions and unit/property tests cover round trips and boundaries. The implementation preserves the predecessor’s eight-digit low-half format, and the saved compatibility test covers unpadded persisted LSNs.
- Q1097-R05: The module is imported by production code. The named version, scheduler, CDC, recovery/refresh, and WAL paths call it or checked adapters. The caller changes inspected use numeric comparisons and return or handle parse errors.
- **F1 — Runtime coverage gap (Q1097-R06, Q1097-R07).** [The packaged E2E case](/private/tmp/issue1097-candidate-b2_6bc63/tests/e2e_lsn_contract_tests.rs:33) tests PostgreSQL numeric ordering, an unpadded real WAL LSN, recovery validation, and manual refresh failure without changing rows or stored progress. It does not exercise malformed progress through the scheduler watermark, CDC holdback, or WAL transition workflows required by these rows. The WAL E2E module is disabled at [tests/e2e_wal_cdc_tests.rs:18](/private/tmp/issue1097-candidate-b2_6bc63/tests/e2e_wal_cdc_tests.rs:18); the CDC state unit test does not exercise the production database caller. Add focused runtime cases through those named workflows, asserting valid boundary behavior and that malformed inputs leave committed progress, buffers, and public results unchanged.
- Q1097-R08: The saved Verus record reports 44 verified and zero errors. Its source digest and baseline/mutant log hashes match the checked files. The executable `pack_halves` mutation is recorded as rejected.
- Q1097-R09: The current CI workflow runs the pinned verifier and gates `light-e2e-package` on it. The new negative-control JSON and log record all three expected rejections. Their candidate source hash, workflow hash, and log hash match the current files; inspection of the retained temporary roots confirms the private obligation, removed package dependency, and fake zero-exit verifier inputs. The older saved proof’s statement that these controls were absent is stale.
- Q1097-R10: This report independently reviews the specification and its assumptions; details appear under Engineering quality.
- Q1097-R11: The saved binding record names this exact snapshot and source digest. Its package and installed `pg_trickle.so` digests match in the PostgreSQL 18.6 attestation.

## Scope and simplicity

No material findings. The Verus source and dependency, runner, CI gate, package binding checks, and focused E2E target serve explicit contract requirements. I found no unrelated product behavior in the diff.

## Engineering quality

- **F2 — Misleading formatter documentation (Q1097-R02, Q1097-R04).** The new module and `format()` comments in [verification/lsn.rs:1](/private/tmp/issue1097-candidate-b2_6bc63/verification/lsn.rs:1) and [verification/lsn.rs:979](/private/tmp/issue1097-candidate-b2_6bc63/verification/lsn.rs:979) call `X/XXXXXXXX` PostgreSQL’s canonical output. PostgreSQL 18 accepts one to eight hex digits per component, but `pg_lsn_out` formats with `%X/%X`, without padding the low half. The contract deliberately defines a separate formatter with an eight-digit low half; for example, this code formats position 1 as `0/00000001`, while PostgreSQL outputs `0/1`. The implementation matches the contract and preserves the predecessor format, but the new comment can lead callers to assume textual equality with PostgreSQL output. Describe this as the contract’s canonical format. The cited [PostgreSQL 18 source](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/pg_lsn.c) confirms both the component limits and `%X/%X` output.
- I checked `verification/lsn-specification.md` at SHA-256 `d181e07582dfbc490ffa5e08fb329209217a898fd52c3e0d471986e38388382e` against `verification/lsn.rs` at SHA-256 `3295bf53ed0adb1fdc6d003f797a7e17f85cf24002bf3ea1ff8f10451daec849` and the PostgreSQL 18 source. Its grammar, mathematical definitions, UTF-8 and allocation trust boundaries, toolchain assumptions, caller inventory, and excluded behavior match the implementation. The previous saved review’s claim that PostgreSQL uses `%X/%08X` is false; it conflates PostgreSQL output with the contract formatter. This report records the corrected independent review. It has not been saved because the requested stage is read-only.

## Checks and limitations

- The read-only candidate validator exited successfully. It confirmed the snapshot key, all 1,564 manifest entries, base SHA, contract SHA, and five binding-input hashes. The local and external `candidate.json` files are byte-identical.
- The live-read snapshots show #1090 open with no comments; its child checklist leaves #1097 unchecked. No parent `slicing.md` or approval receipt was found in the inspected work records. The supplied base is confirmed as the requested comparison base, but not as the approved destination tip.
- The saved Verus record and logs, focused packaged E2E log, candidate-binding record, installation attestation, implementation report, prior review, and both proof reports were inspected. The focused E2E log reports 4/4 tests passed, including one LSN integration test. The saved full light-E2E run was incomplete at 1,047 of 1,448 tests. I did not rerun tests or builds.
- The prior review and proof reports were treated as claims to check. The proof-after-review R09 observation predates the new negative-control evidence; its R06/R07 coverage gap remains.
- No files were modified. The parent routing remains unresolved, so this report does not confirm the requested base as the approved delivery target.

## Handoff

F1 affects Q1097-R06 and Q1097-R07. F2 corrects an in-scope formatter claim. No contract amendment is indicated. Resolve the missing #1090 slicing and approval route before treating the base as the approved target.

Report storage: Proposed [review.md](/private/tmp/issue1097-candidate-b2_6bc63/.p2p/work/issue-1097-verified-production-lsn/review.md); storage pending because this review was read-only.

Review only; acceptance proof and merge readiness are separate.

## Next steps

1. In a write-authorized stage, save this exact report as `.p2p/work/issue-1097-verified-production-lsn/review.md` and read it back. Run `/slice-contract #1090` to restore the canonical decomposition, approved delivery plan, and retrievable approval receipt. Confirm its destination tip equals `6dc7e07c6bf53df2bf2327c4146cb140ca433f64`; if it differs, refresh the review against the resolved tip.
2. Run `/implement-contract work/issue-1097-verified-production-lsn.md; findings .p2p/work/issue-1097-verified-production-lsn/review.md` for F1 and F2.
3. After changes, capture the exact new candidate and run a fresh full `/review-implementation` and `/prove` against revision v1 and the resolved comparison base.