# CHANGES NEEDED: issue #1097 verified production LSN

Contract: `work/issue-1097-verified-production-lsn.md`, revision v1, SHA-256 `dae07ec8c073477626add9b70a114891b5bbe7431e9790537c075bd731cc4259`.

Parent context: #1090 v1, SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`; contribution maps to Q1090-R03, Q1090-R05, and Q1090-R06. The retained #1091 and #1093 snapshots record their prior outcomes; they do not establish this candidate’s behavior. No recoverable #1090 slicing record or approved delivery plan was present, so target routing is not confirmed.

Candidate: `snapshot:sha256:3e8a030c39c44af4c090e4677ddcacd5a4659d422ff17a20e86edbb902df1018`; recoverable manifest in [candidate.json](/Users/grove/projects/pg-trickle2/.p2p/work/issue-1097-verified-production-lsn/candidate.json).

Comparison: base `6dc7e07c6bf53df2bf2327c4146cb140ca433f64`; the requested complete working-tree snapshot, including untracked product files and excluding `.p2p/`.

Stability: Candidate, contract, binding inputs, and base revalidated at the end of review. The candidate record validates with 1,564 manifest entries; `HEAD` equals the requested base. The candidate record copies in both checkouts have the same SHA-256.

Coverage: All Q1097-R01–Q1097-R11 were inspected. R01–R03: checked parser, formatter, ordering, and named verifier obligations. R04: inspected persisted-string compatibility coverage. R05: traced the named production caller inventory. R06–R07: F1. R08–R09: inspected mutation control and CI enforcement. R10: independently reviewed the specification as part of this report. R11: inspected source, package, and installed-library binding. No proof verdicts are assigned. Target routing remains unchecked as described above.

## Contract fidelity

- **F1 — Runtime coverage gap (Q1097-R06, Q1097-R07).** [The E2E test](/private/tmp/issue1097-candidate-b2_6bc63/tests/e2e_lsn_contract_tests.rs:33) exercises predecessor spelling, public recovery, and manual refresh. It does not exercise the scheduled watermark path, CDC holdback through its database caller, or WAL transition workflow. [The WAL E2E module](/private/tmp/issue1097-candidate-b2_6bc63/tests/e2e_wal_cdc_tests.rs:17) is disabled with `#![cfg(any())`. The [implementation report](/Users/grove/projects/pg-trickle2/.p2p/work/issue-1097-verified-production-lsn/implementation.md) also identifies these paths as unverified. As a result, the candidate does not establish the promised fail-closed behavior and unchanged progress through those named callers. Add focused runtime cases for malformed inputs and valid boundary ordering through the scheduler, CDC holdback, and WAL transition paths, asserting that invalid inputs do not advance progress.

## Scope and simplicity

No material findings.

## Engineering quality

No additional material findings. I independently reviewed the specification for Q1097-R10 as part of this review. Reviewer: Codex, independent of the implementation stage. Specification SHA-256: `d181e07582dfbc490ffa5e08fb329209217a898fd52c3e0d471986e38388382e`. The accepted grammar and formatter agree with PostgreSQL 18’s `pg_lsn` implementation, including its 1–8 hex-digit component limits and `%X/%08X` output format. The source, mathematical definitions, and documented UTF-8 and formatter trust boundaries are consistent with the inspected implementation. [PostgreSQL 18 `pg_lsn.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/pg_lsn.c). This report records that review; its storage and readback remain pending.

## Checks and limitations

- The read-only `p2p_filesystem.py validate` check confirmed the requested snapshot key, base, contract hash, and all binding-input hashes.
- The saved Verus output records `44 verified, 0 errors`; the semantic mutation output records `43 verified, 1 error`. The verification record’s source digest matches `verification/lsn.rs`.
- The saved focused E2E log reports 4/4 tests passed on PostgreSQL 18.6. The candidate-binding record matches the checked source digest and reports matching packaged and installed `pg_trickle.so` digests.
- The saved full light-E2E run was interrupted after 1,047 of 1,448 tests; 18 were skipped and 401 were not run. The implementation report records `just fmt`, `just lint`, unit, and integration results. I did not rerun builds or tests.
- No #1090 `.p2p/work/.../slicing.md` or approval receipt was available. The supplied base is validated as the requested comparison base, but its match to an approved destination tip is unconfirmed.
- No files were changed. Review is against the captured candidate and does not establish acceptance or merge readiness.

## Handoff

F1 affects Q1097-R06 and Q1097-R07 and belongs in an authorized `implement-contract` handoff. No contract amendment is indicated. Restore the missing parent routing context before treating this base as the approved delivery target.

Report storage: proposed `/private/tmp/issue1097-candidate-b2_6bc63/.p2p/work/issue-1097-verified-production-lsn/review.md`; storage pending because this is a read-only stage.

Review only; acceptance proof and merge readiness are separate.

## Next steps

1. Restore the canonical #1090 parent work item, its `.p2p/work/<parent>/slicing.md`, and the linked approval receipt. The approved plan should name the child work item, resolved destination, and full target-tip SHA. If that tip differs from `6dc7e07c6bf53df2bf2327c4146cb140ca433f64`, review against the resolved tip.
2. After this report is saved and reread at the proposed path, run `/implement-contract work/issue-1097-verified-production-lsn.md; findings .p2p/work/issue-1097-verified-production-lsn/review.md` to address F1.
3. Capture the changed candidate, then run a fresh full `/review-implementation` and `/prove` for the same contract, new exact candidate identity, and resolved comparison base.