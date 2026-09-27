# REVIEWED: Preserve downstream consistency across IMMEDIATE refresh and truncate paths

**Contract:** [work/issue-1119-immediate-downstream-consistency.md](/Users/grove/projects/pg-trickle3/work/issue-1119-immediate-downstream-consistency.md), v1, SHA-256 `c7ecbeb273d2fd8ed8ef6ef3cc31fd0f2128550049a1daea93896bf7626592b8`.

**Parent context:** None. I inspected the required cascading IVM and repair mechanisms.

**Candidate:** `snapshot:sha256:08f36577aca0522020d99d2bad996917b98746f08d0875bb06fb3596176405c9`, reconstructed at `/private/tmp/pg-trickle-issue1119-candidate-final-08f36577`.

**Comparison:** Base `2dfe8dae90452120cf6760b71dd0def8c241756b`, exported at `/private/tmp/pg-trickle-issue1119-base`. The product diff contains 24 changed or added paths, including the release migration and archive.

**Stability:** The contract hash matches. The candidate manifest records the supplied full key and base; all 1,556 entries match the reconstructed files and modes, with no extra paths or mismatches. The binding hashes for `AGENTS.md` and `work/sources/issue-1119.md` also match. No drift was observed.

**Coverage:** Reviewed R1–R8, caller paths, migration and recovery documentation, trigger tests, keyless duplicate capture, and saved evidence. No material change-required findings.

## Contract fidelity

No material findings.

- **R1–R3:** Manual and scheduled FULL callers suppress application triggers while the shared helper preserves IVM maintenance triggers and restores each application trigger’s prior mode. The tests cover immediate descendants, same-transaction results, empty replacement, actual scheduled FULL history, application side effects, and `O/D/R/A` trigger modes. The reserved-prefix-only case exercises the actual helper gate.
- **R4 and F1:** `capture_diff_to_table` pairs duplicate occurrences by row identity and occurrence number in both delta-scoped and full-snapshot paths. The keyless scan’s delta identity matches full materialization. The truncate regression exercises duplicate rows through incremental immediate capture and FULL capture, plus NULLs, empty/repeated truncation, aggregate zero rows, separate consumers, and later changes.
- **R5:** The final regression test checks explicit rollback and injected FULL and TRUNCATE capture failures. It asserts the intended CHECK error and compares rows, buffers, frontiers, and trigger states from fresh connections. Successful retries are also checked through fresh connections. The saved final R5 log reports **1 passed, 0 failed**.
- **R6:** The consuming deferred refresh asserts `DIFFERENTIAL`, rejects FULL fallback, and checks exact results after the first and repeated refresh.
- **R7:** The guide separates code installation, migration, and repair; gives dependency-order recovery instructions, including for installations already at v0.108.1. The test checks base-derived results after each repair and verifies a subsequent write before commit.
- **R8:** The v0.108.0→v0.108.1 migration and archived install SQL update `quick_health`. The upgrade test covers a real extension update, view shape and grant retention, healthy, missing, disabled, and repaired states, and comparison with a fresh installation.

**F1 duplicate capture:** The occurrence-pairing implementation applies to both delta-scoped and full capture. The truncate test’s duplicate insert exercises the immediate delta path; its FULL stream-table refresh exercises full capture.

**F2 function identity:** Production detection joins each trigger’s `tgfoid` to `pg_proc`, checks the `pgtrickle` namespace, and matches the maintenance-function pattern. The SQL integration test mirrors that predicate rather than invoking the Rust helper directly; the reserved-prefix-only FULL-refresh test exercises the actual gate end to end.

## Scope and simplicity

No material findings. The 24-path diff stays focused on refresh and capture behavior, recovery guidance, the health-view migration, tests, and v0.108.1 release metadata. I found no new public configuration or general-purpose framework.

## Engineering quality

No material findings. The capture runs within the originating transaction; helper locking and trigger restoration are consistent across manual and scheduled paths. SQL comparisons preserve duplicate multiplicity with `EXCEPT ALL`.

## Checks and limitations

I ran no tests and made no writes. Saved final R5 evidence is at [issue-1119-r5-final-e2e.log](/Users/grove/projects/pg-trickle3/.p2p/work/issue-1119-immediate-downstream-consistency/evidence/issue-1119-r5-final-e2e.log). The final fmt and lint logs record formatting, Clippy with warnings denied, and the repository lint checks passing.

Broader retained E2E and upgrade logs exist, but they do not establish that the full suite ran against this exact final snapshot. The saved [proof.md](/Users/grove/projects/pg-trickle3/.p2p/work/issue-1119-immediate-downstream-consistency/proof.md) binds an older candidate key, `d755ec404d1d03d9417211df4a150b72888bd944ae53f9a78cd9026c5b541939`; I did not use its verdicts for this review. No PR reference was provided, so publication and merge readiness were not assessed.

The environment grants `workspace-write`; I treated the inputs as read-only per task instruction and made no changes.

## Handoff

No in-scope findings to hand back. This review is not acceptance proof or merge approval.

**Report storage:** Pending. Return this report to the enclosing workflow to save and reread at `.p2p/work/issue-1119-immediate-downstream-consistency/review.md`.

## Next steps

1. Save and reread this report at the review path above.
2. Run `/prove work/issue-1119-immediate-downstream-consistency.md; candidate snapshot:sha256:08f36577aca0522020d99d2bad996917b98746f08d0875bb06fb3596176405c9` for a matching acceptance result.
